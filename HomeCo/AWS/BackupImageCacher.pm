package HomeCo::AWS::BackupImageCacher;

use 5.10.0;
use strict;
use warnings;
use utf8;

use base qw{ Exporter };
our @EXPORT = qw{ backup check_parameters parameter_match ping_metadata_store close_metadata_store open_metadata_store cleanup };

=head1 NAME

HomeCo::AWS::BackupImageCacher - Backups images using Amazon Glacier on a daily or monthly bases.

=head1 VERSION

Version 0.1

=cut

our $VERSION = '0.1';

use Net::Amazon::Glacier 0.14;
use File::Find;
use File::Spec;
use File::Temp;
use DateTime;
use Carp;
use DBI;
use Archive::Tar::Streamed;
use Tie::FileHandle::Split;
use Cwd;

# tar sizes from http://www.gnu.org/software/tar/manual/html_node/Standard.html#SEC184
my $tar_record_size = 512; # standard metadata size and data block size for padding
my $tar_blocking_factor = 20; # block to round up output file
my $tar_block_size = $tar_blocking_factor * $tar_record_size; # size in bytes of output file round up

sub backup ( $ ) {
	my $config = shift;

	eval {
		# try to control parameters are checked. Can be circunvent, thou...
		check_parameters( $config ) unless defined $config->{_checked};
		HomeCo::AWS::BackupImageCacher::_backup( $config );
	};
	if ( $@ ) {
		die( $@ );
	}
}

sub parameter_match ( $ ) {
	my $config = shift;
	return 	(
		"vault=s" => \$config->{VaultName},
		"region=s" => \$config->{VaultRegion},
		"credentials=s" => \$config->{AWSCredentials},
		"daily" => \$config->{Daily},
		"monthly" => \$config->{Monthly},
		"date=s" => \$config->{Date},
		"cleanup" => \$config->{Cleanup},
	);
}

sub check_parameters ( $ ) {
	my $config = shift;

	die("BaseThumbs not specified.") unless defined $config->{BaseThumbs};
	die("BaseThumbs does not exist.") unless -d $config->{BaseThumbs};
	die("BaseImageCache not specified.") unless defined  $config->{BaseImageCache};
	die("BaseImageCache does not exist.") unless -d $config->{BaseImageCache};
	die("VaultRegion not specified. Set an AWS Region.") unless defined $config->{VaultRegion} && $config->{VaultRegion} ne '';
	die("VaultName not specified. Set an existing AWS Glacier Vault.") unless defined $config->{VaultName} && $config->{VaultName} ne '';
	die("AWSCredentials file does not exist. Provide valid AWS Credetials-") unless -e $config->{AWSCredentials};

	# either daily or monthly
	die("Either daily, monthly or cleanup must be specified.") if ( !( $config->{Daily} || $config->{Monthly} || $config->{Cleanup} ) );
	die("Cannot request daily and monthly backup in a single run.") if ( $config->{Daily} && $config->{Monthly} );

	# check date is valid
	my ($year, $month, $day);

	die("Provided date is invalid " . $config->{Date} . ".") unless eval {
		if ( $config->{Daily} ) {
			($year, $month, $day) = unpack "A4 A2 A2", $config->{Date};
			$config->{_thumbs_backup_path} = File::Spec->catpath( $config->{BaseThumbs}, "$year$month$day" );
			$config->{_images_backup_path} = File::Spec->catpath( $config->{BaseImageCache}, "$year$month$day" );
			$config->{Comment} = 'DAILY_' . "$year$month$day";

		} elsif ( $config->{Monthly} ) {
			($year, $month) = unpack "A4 A2", $config->{Date};
			$day = 1; #ensure date exists to check just month and year
			$config->{_thumbs_backup_path} = File::Spec->catpath( $config->{BaseThumbs}, "$year$month" );
			$config->{_images_backup_path} = File::Spec->catpath( $config->{BaseImageCache}, "$year$month" );
			$config->{Comment} = 'MONTHLY_' . "$year$month";
		}
		#check dat exists in calendar
		DateTime->new( year => $year,  month => $month, day => $day );

		1;
	};

	die("Thumbs directory does not exists at " . $config->{_thumbs_backup_path} ) unless ( -d $config->{_thumbs_backup_path} );
	die("Images directory does not exists at " . $config->{_images_backup_path} ) unless ( -d $config->{_images_backup_path} );

	$config->{_checked} = 1;

	return $config->{_checked};
}

sub ping_metadata_store ( $ ) {
	my $config = shift;

	eval {
		$config->{dbh}->do( $config->{SQLPing} );
	};

	return $@;
}

sub open_metadata_store ( $ ) {
	my $config = shift;

	eval {
		eval {
			my $dbh = DBI->connect( $config->{DatabaseConnect}, $config->{DatabaseUsername}, $config->{DatabasePassword}, { RaiseError => 1 } );

			$config->{dbh} = $dbh;

			#try to select metadata store
			$config->{dbh}->do( $config->{SQLSelectTable} );
		};
		if ( $@ ) {
			#on error try to create
			eval {
				$config->{dbh}->do( $config->{SQLCreateTable} );
			};
			if ( $@ ) {
				#die if metadata is innaccessible and we cannot create store
				die( $@ );
			}
		}
	};
	if ( $@ ) {
		#could not connect
		die( $@ );
	}
}

sub close_metadata_store( $ ) {
	my $config = shift;

	$config->{dbh}->disconnect;
}

sub cleanup( $ ) {
	my $config = shift;

	my $glacier = Net::Amazon::Glacier->new(
		$config->{VaultRegion},
		$config->{AWSAccessKey},
		$config->{AWSSecret}
	);

	#check Vault exists
	die ( 'Vault does not seem to exist' ) unless eval {
		$glacier->describe_vault( $config->{VaultName} );
		1;
	};

	#check last monthly
	my $monthly = $config->{dbh}->selectrow_hashref( $config->{SQLSelectLastMonthly} );

	unless ( $monthly->{archive_id} ) {
		if ( $monthly->{archive_id} =~ /^MONTHLY_(\d\d\d\d)(\d\d)$/ ) {
			my ( $year, $month) = ( $1, $2 );

			# like statement match for DAILY_YYYYMM??
			my $daily_description = 'DAILY_' . $year . $month . '%';

			my $sth_old_dailies = $config->{dbh}->prepare( $config->{SQLSelectByDescription} );
			$sth_old_dailies->execute( $daily_description );

			while ( my $old_daily = $sth_old_dailies->fetchrow_hashref() ) {
				$config->{ArchiveId} = $old_daily->{archive_id};
				if ( _delete( $config ) ) {
					#****log deleted
					my $rows_deleted = $config->{dbh}->do( $config->{SQLDeleteSingleArchive}, undef, $old_daily->{archive_id} );
					if ( $rows_deleted == 1 ) {
						#***log deleted archive
					} else {
						#***log odd archives deleted
					}

				} else {
					#****log not deleted
				}
			}

			# get all possible
		} else {
			#bad description in last monthly
			die("Wrong monthly description format");
		}
	} else {
		#***log no last monthly...
		#no monthly, ever, no cleanup needed
	}
}

sub _delete() {
	my $config = shift;

	my $glacier = Net::Amazon::Glacier->new(
		$config->{VaultRegion},
		$config->{AWSAccessKey},
		$config->{AWSSecret}
	);

	return $glacier->delete_archive( $config->{Vault}, $config->{ArchiveId} );
}

sub _backup() {
	my $config = shift;

	my $glacier = Net::Amazon::Glacier->new(
		$config->{VaultRegion},
		$config->{AWSAccessKey},
		$config->{AWSSecret}
	);

	#check Vault exists
	die ( 'Vault does not seem to exist' ) unless $config->{NoGlacierAPICalls} || eval {
		$glacier->describe_vault( $config->{VaultName} );
		1;
	};

	my $retry = 0;
	my $archive_id = undef;

	while ( $retry++ <= $config->{RetryBeforeError} && !defined $archive_id ) {
		my $file_size = 0;
		#estimate part size for estimated tarred directory size
		my $tarred_size = _tar_directory_size( [ $config->{_thumbs_backup_path}, $config->{_images_backup_path} ] );
		my $part_size = $glacier->calculate_multipart_upload_partsize( $tarred_size );

		my $current_upload_id = $glacier->multipart_upload_init( $config->{VaultName}, $part_size, $config->{Comment} ) unless $config->{NoGlacierAPICalls};

		my $part_index = 0;
		my $parts_hash = [];

		# Cleanup only when not debugging
		my $dir = File::Temp::tempdir( CLEANUP => ( $config->{Debug}?0:1 ) );

		tie *TAR, 'Tie::FileHandle::Split', $dir, $part_size;

		# Gets called when a part is generated by the streaming tar
		(tied *TAR)->add_file_creation_listeners( sub {
			my ( $tied_filehandle, $current_part_temp_path  ) = @_;
			
			# compute last file size, most will be part_size, last might not
			$file_size += -s $current_part_temp_path;
			
			eval {
				$parts_hash->[$part_index] = $glacier->multipart_upload_upload_part( $config->{VaultName}, $current_upload_id, $part_size, $part_index, $current_part_temp_path ) unless $config->{NoGlacierAPICalls};
			};
			croak( $@ ) if ( $@ );
			#***log, do not die
			die( "Not a valid part hash" ) unless $config->{NoGlacierAPICalls} || $parts_hash->[$part_index] =~ /^[0-9a-f]{64}$/;
			
			#go to next part index next time
			$part_index++;
			
			# dispose temp file when not debugging
			unlink $current_part_temp_path unless $config->{Debug};
		});

		# This calls the listener for each part
		_tar_directory_contents( \*TAR, [ $config->{_thumbs_backup_path}, $config->{_images_backup_path} ] );

		# This could call the listener for the last part
		(tied *TAR)->write_buffers();

		#$archive_id undef unless completed
		eval {
			$archive_id = $glacier->multipart_upload_complete( $config->{VaultName}, $current_upload_id, $parts_hash, $file_size ) unless $config->{NoGlacierAPICalls};
		};

		if ( defined $archive_id ) {
			#***log archive_id and execute data
			my $sth = $config->{dbh}->prepare( $config->{SQLInsertSingleArchive} );
			$sth->execute(
				$archive_id,
				$config->{Date},
				Cwd::abs_path( $config->{_thumbs_backup_path} ) . ' & ' . Cwd::abs_path( $config->{_images_backup_path} ),
				DateTime->now->ymd('') ) unless $config->{NoGlacierAPICalls};
		} else {
			# ***log
			die("Bad archive_id generated.") unless $config->{NoGlacierAPICalls} || defined $archive_id;
		}
	}

	return $archive_id;
}

# Split input from file handle into n part_size files
sub _store_file_part() {
	my ( $config, $fh, $part_size ) = @_;

	my ($temp_fh, $temp_name) = File::Temp::mkstemp( );

	binmode $temp_fh;

	my $at = 0;
	my $buf;

	# read parts as long a reading a part will not get us past a part_limit
	# or until eof
	while ( $at + $config->{ReadBufferSize} <= $part_size && !$fh->eof ) {
		read( $fh, $buf, $config->{ReadBufferSize} );
		$temp_fh->print( $buf );
		$at += $config->{ReadBufferSize};
	}

	unless ( $fh->eof ) {
		# calculate remaing bytes to read until part_size in case ReadBufferSize does
		# not divide part_size evenly
		my $last_buffer_size = $part_size - $at;

		if ( $last_buffer_size > 0 ) {
			read( $fh, $buf, $config->{ReadBufferSize} );
			$temp_fh->print( $buf );
			$at += $config->{ReadBufferSize};
		}
		# at can only differ in last part, since this is not eof croack on non part_size at's
		croack( "We did not get to a part_size while reading" ) unless ( $at == $part_size );
	}

	return $temp_name;
}

sub _tar_archive_member_size($) {
	# separate and testable
	my $size = shift;

	# Physically, an archive consists of a series of file entries terminated by an end-of-archive entry,
	# which consists of two 512 blocks of zero bytes. A file entry usually describes one of the files
	# in the archive (an archive member), and consists of a file header and the contents of the file.
	# File headers contain file names and statistics, checksum information which tar uses to detect file
	# corruption, and information about file types.

	# archive member = record size (which is 512) + end-of-archive ( 2 * record size ) + file contents rounded up to next record size boundry.
	return 3 * $tar_record_size + POSIX::ceil( $size / $tar_record_size ) * $tar_record_size;
}

sub _tar_output_file_size($) {
	# separate and testable
	my $size = shift;

	# From http://www.gnu.org/software/tar/manual/html_node/Standard.html#SEC184
	# A tar archive file contains a series of blocks. Each block contains BLOCKSIZE bytes.

	# Round up to next tar block size
	$size = POSIX::ceil( $size / $tar_block_size ) * $tar_block_size;

	# check minimum size of output file is met or pad
	$size = $tar_block_size if ( $size < $ tar_block_size);

	return $size;
}

sub _tar_directory_size ($) {
	# File path are somehow limited. If path are to long additional info blocks might be generated by tar, since we are streaming we could get into trouble with maximum file pieces

	my ( $dirs ) = @_;

	my $size_sum = 0; # adds metadata blocks and archive content size with proper rounding

	# -s returns 0 for directories, which is consistent

	while ( my $dir = shift @$dirs) {
		die( "Directory does not exist" ) unless -d $dir;
		File::Find::find( sub { $size_sum += _tar_archive_member_size( -s $_) unless (-d $_); }, $dir );
	}

	return _tar_output_file_size( $size_sum );
}

sub _tar_directory_contents ($$) {
	my ( $fh, $dirs ) = @_;

  my $tar = Archive::Tar::Streamed->new( $fh );

	while ( my $dir = shift @$dirs) {
		die( "Directory does not exist" ) unless -d $dir;
		File::Find::find( {
			no_chdir => 1,
			wanted => sub {
				# Do not add . and ..
				# ***log added file
				unless ( -d $File::Find::name ) {
					$tar->add( $File::Find::name );
				}
			}
		}, $dir );
	}
}

1;