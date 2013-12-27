package HomeCo::AWS::BackupImageCacher;

use 5.10.0;
use strict;
use warnings;

use base qw{ Exporter };
our @EXPORT = qw{ backup check_parameters parameter_match open_matadata_store ping_metadata_store close_matadata_store cleanup};

=head1 NAME

HomeCo::AWS::BackupImageCacher - Backups images using Amazon Glacier on a daily or monthly bases.

=head1 VERSION

Version 0.1

=cut

our $VERSION = '0.1';

use File::Find;
use File::Spec;
use File::Temp;
use Net::Amazon::Glacier 0.13;

my $tar_command = 'c:\Program Files (x86)\GnuWin32\bin\tar -cf - '; #command less paths (do not delete trailing space)

# tar sizes from http://www.gnu.org/software/tar/manual/html_node/Standard.html#SEC184
my $tar_record_size = 512; # standard metadata size and data block size for padding
my $tar_blocking_factor = 20; # block to round up output file
my $tar_block_size = $tar_blocking_factor * $tar_record_size; # size in bytes of output file round up

sub backup () {
	my $config = $_;

	# try to control parameters are checked. Can be circunvent, thou...
	check_parameters( $config ) unless defined $config->{_checked};

	eval {
		HomeCo::AWS::BackupImageCacher::_backup( $config );
	};
	if ( $@ ) {
		die( $@ );
	}
}

sub parameter_match () {
	return 	(
		"vault=s" => $config->{VaultName},
		"region=s" => $config->{VaultRegion},
		"credentials=s" => $config->{AWSCredentials},
		"daily" => $config->{Daily},
		"monthly" => $config->{Monthly},
		"date=s" => $config->{Date},
		"cleanup" => $config->{Cleanup},
	);
}

sub check_parameters () {
	my $config = $_;

	die("No BaseThumbs defined.") unless defined $config->{BaseThumbs};
	die("BaseThumbs does not exist.") unless -d $config->{BaseThumbs};
	die("No BaseImageCache defined.") unless defined  $config->{BaseImageCache};
	die("BaseImageCache does not exist.") unless -d $config->{BaseImageCache};
	die("VaultRegion does not exist. Set an AWS Region.") unless -d $config->{VaultRegion};
	die("VaultName does not exist. Set an existing AWS Glacier Vault.") unless -d $config->{VaultRegion};
	die("AWSCredentials file does not exist. Provide valid AWS Credetials-") unless -e $config->{AWSCredentials};

	# either daily or monthly
	die("Cannot request daily and monthly backup in a single run.") if ( $config->{Daily} && $config->{Monthly} );
	die("Either daily, monthly or cleanup must be specified.") if ( !( $config->{Daily} || $config->{Monthly} || $config->{Cleanup} );

	# check date is valid
	my ($year, $month, $day);

	die("Provided date is invalid " . $config->{Date} . ".") unless eval {
		if ( $config->{Daily} ) {
			($year, $month, $day) = unpack "A4 A2 A2", $config->{Date};
			$config->{_thumbs_backup_path} = File::Spec::catpath( $config->{BaseThumbs}, "$year$month$day" );
			$config->{_images_backup_path} = File::Spec::catpath( $config->{BaseImageCache}, "$year$month$day" );
			$config->{CommentTrail} = 'DAILY_';

		} elsif ( $config->{Monthly} ) {
			($year, $month) = unpack "A4 A2", $config->{Date};
			$day = 1; #ensure date exists to check just month and year
			$config->{_thumbs_backup_path} = File::Spec::catpath( $config->{BaseThumbs}, "$year$month" );
			$config->{_images_backup_path} = File::Spec::catpath( $config->{BaseImageCache}, "$year$month" );
			$config->{CommentTrail} = 'MONTHLY_';
		}
		#check dat exists in calendar
		timelocal(0,0,0,$day, $month-1, $year);

		1;
	};

	die("Thumbs directory does not exists at " . $config->{_thumbs_backup_path} ) unless -d $config->{_thumbs_backup_path};
	die("Images directory does not exists at " . $config->{_images_backup_path} ) unless -d $config->{_images_backup_path};

	# check nobody deleted the trailing space in command
	$tar_command += ' ' if ( substr($tar_command,-1,1) ne ' ' );

	$config->{_checked} = 1;

	return $config->{_checked};
}

sub ping_metadata_store() {
	my config = $_;

	eval {
		$config->{dbh}->do( SQLPing );
	};

	return $@;
}

sub open_matadata_store () {
	my config = $_;

	eval {
		eval {
			my $dbh = DBI->connect( $config->{DatabaseConnect}, $config->{DatabaseUsername}, $config->{DatabasePassword}, { RaiseError => 1 } );

			#try to select metadata store
			$dbh->do( $config->{SQLSelectTable} );
		};
		if ( $@ ) {
			#on error try to create
			eval {
				$dbh->do( $config->{SQLCreateTable} );
			} if ( $@ ) {
				#die if metadata is innccessible and we cannot create store
				die( $@ );
			}
		}
	};
	if ( $@ ) {
		#could not connect
		die( $@ );
	}
}

sub close_matadata_store() {
	my $config = $_;

	$config->{dbh}->disconnect;
}

sub cleanup() {
	my $config = $_;

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






}

sub _backup() {
	my $config = $_;

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

	my retry = 0;

	while ( retry++ <= $config->{RetryBeforeError} ) {

		#estimate part size for estimated tarred directory size
		my $part_size = $glacier->calculate_multipart_upload_partsize( _tar_size_directory( $config->{_thumbs_backup_path} ) );

		my $current_upload_id = $glacier->multipart_upload_init( $config->{VaultName}, $part_size, $config->{CommentTrail} . $config->{_thumbs_backup_path} );

		my $part_index = 0;
		my $parts_hash = [];

		while( !$fh->eof ) {
			my $current_part_temp_path = _store_file_part( $part_size );

			$parts_hash->[$part_index] = $glacier->multipart_upload_upload_part( $config->{VaultName}, $current_upload_id, $part_size, $part_index, $current_part_temp_path );

			die( "Not a valid part hash" ) unless $parts_hash->[$part_index] =~ /^[0-9a-f]{64}$/;

			# compute last file size, most will be part_size, last might not
			$file_size += -s $current_part_temp_path

			# dispose temp file
			unlink $current_part_temp_path;
		}
	}

	# complete upload
	my $archive_id = $glacier->multipart_upload_complete( $config->{VaultName}, $current_upload_id, parts_hash, $file_size );

	my $sth = $config->{dbh}->prepare( $config->{SQLInsertSingleArchive} );
	******
	$sth->execute( );

	#***log archive_id



	return $archive_id;
}

sub _tar_size_directory () {
	# file path are somehow limited. If path are to long additional info blocks might be generated by tar, since we are streaming we could get into trouble with maximum file pieces

	my $dir = $_;

	die( "Directory does not exist" ) unless -d $dir;

	my $size_sum = 0; # adds metadata blocks and archive content size with proper rounding

	# -s returns 0 for directories, which is consistent

	File::Find::find( { $size_sum += _tar_archive_member_size( -s $_) }, $dir );

	return $size_sum;
}

# Split input from file handle into n part_size files
sub _store_file_part() {
	my ( $fh, $part_size ) = @_;

	my ($temp_fh, $temp_name) = File::Temp::mkstemp( );

	my $at = 0;
	my $buf;

	# read parts as long a reading a part will not get us past a part_limit
	# or until eof
	while ( $at + $config->{ReadBufferSize} <= $part_size || $fh->eof ) {
		read( $fh, $buf, $config->{ReadBufferSize} );
		write( $temp_fh, $buf );
		$at += $config->{ReadBufferSize};
	}

	unless ( $fh->eof ) {
		# calculate remaing bytes to read until part_size in case ReadBufferSize does
		# not divide part_size evenly
		my $last_buffer_size = $part_size - $at;

		if ( $last_buffer_size > 0 ) {
			read( $fh, $buf, $config->{ReadBufferSize} );
			write( $temp_fh, $buf );
			$at += $config->{ReadBufferSize};
		}
		# at can only differ in last part, since this is not eof croack on non part_size at's
		croack( "We did not get to a part_size while reading" ) unless ( $at == $part_size );
	}

	return $temp_name;
}

sub _tar_archive_member_size() {
	# separate and testable
	my $size = $_;

	# Physically, an archive consists of a series of file entries terminated by an end-of-archive entry,
	# which consists of two 512 blocks of zero bytes. A file entry usually describes one of the files
	# in the archive (an archive member), and consists of a file header and the contents of the file.
	# File headers contain file names and statistics, checksum information which tar uses to detect file
	# corruption, and information about file types.

	# archive member (512) + end-of-archive ( 2 * 512 ) + file contents rounded up to next 512 boundry.
	return 3 * $tar_block_size + ceil( $size / $tar_block_size ) * $tar_block_size;
}

sub _tar_output_file_size() {
	# separate and testable
	my $size = $_;

	# from http://www.gnu.org/software/tar/manual/html_node/Standard.html#SEC184
	# A tar archive file contains a series of blocks. Each block contains BLOCKSIZE bytes.

	# round up to next tar block size
	$size = ceil( $size / $tar_block_size ) * $tar_block_size;

	# check minimum size of output file is met or pad
	$size = $tar_block_size if ( $size < $ tar_block_size);

	return $size;
}
