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

# Tar sizes from http://www.gnu.org/software/tar/manual/html_node/Standard.html#SEC184
my $tar_record_size = 512; # standard metadata size and data block size for padding
my $tar_blocking_factor = 20; # block to round up output file
my $tar_block_size = $tar_blocking_factor * $tar_record_size; # size in bytes of output file round up

sub backup ( $ ) {
	my $config = shift;

	eval {
		_log( 'INFO', "Will check parameters.");
		# Try to control parameters are checked. Can be circunvent, thou...
		check_parameters( $config ) unless defined $config->{_checked};
		
		my $archive_id;
		while ( !defined $archive_id ) {
			$archive_id = HomeCo::AWS::BackupImageCacher::_backup( $config );
			if ( !defined $archive_id ) {
				_log( 'ERROR', "Error trying to backup. Will retry forever and email every $config->{RetryBeforeError}.");
			}
		}
	};
	if ( $@ ) {
		_logdie( 'ERROR', "Backup failed with error: $@" );
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
	die("BaseImageCache not specified.") unless defined $config->{BaseImageCache};
	die("BaseImageCache does not exist.") unless -d $config->{BaseImageCache};
	die("VaultRegion not specified. Set an AWS Region.") unless defined $config->{VaultRegion} && $config->{VaultRegion} ne '';
	die("VaultName not specified. Set an existing AWS Glacier Vault.") unless defined $config->{VaultName} && $config->{VaultName} ne '';
	die("AWSCredentials file does not exist. Provide valid AWS Credetials-") unless -e $config->{AWSCredentials};

	# Either daily or monthly
	die("Either daily, monthly or cleanup must be specified.") if ( !( $config->{Daily} || $config->{Monthly} || $config->{Cleanup} ) );
	die("Cannot request daily and monthly backup in a single run.") if ( $config->{Daily} && $config->{Monthly} );

	# Check date is valid
	my ($year, $month, $day);

	die("Provided date is invalid " . $config->{Date} . ".") unless eval {
		if ( $config->{Daily} || $config->{Cleanup} ) {
			($year, $month, $day) = unpack "A4 A2 A2", $config->{Date};
		} elsif ( $config->{Monthly} ) {
			$day = 1; #ensure date exists to check just month and year
			($year, $month) = unpack "A4 A2", $config->{Date};
		}
		#check date exists in calendar
		DateTime->new( year => $year,  month => $month, day => $day );

		1;
	};
	
	if ( $config->{Daily} || $config->{Monthly} || $config->{Cleanup} ) {
		$config->{MonthlyCode} = "$year$month";
	}
	
	if ( $config->{Daily} || $config->{Monthly} ) {
		push @{$config->{backup_files}}, @{_get_local_file_list( $config->{BaseThumbs}, "$year$month$day" )};
		push @{$config->{backup_files}}, @{_get_local_file_list( $config->{BaseImageCache}, "$year$month$day" )};
	}
	
	if ( $config->{Daily} ) {
		$config->{Comment} = 'DAILY_' . "$year$month$day";
		$config->{DailyCode} = "$year$month$day";
		
		# Log files to store
		_log( 'INFO', "Will try to store daily $config->{DailyCode} with these files: " . join( ', ' , @{$config->{backup_files}} ) );
	} elsif( $config->{Monthly} ) {
		$config->{Comment} = 'MONTHLY_' . "$year$month";
		$config->{DailyCode} = undef;

		# Log files to store
		_log( 'INFO', "Will try to store monthly $config->{MonthlyCode} with these files: " . join( ', ' , @{$config->{backup_files}} ) );
	} elsif( $config->{Cleanup} ) {
		# Log action
		_log( 'INFO', "Will try cleanup on monthly $config->{MonthlyCode}" );
	}

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
	my ( $config ) = @_;
	
	_log( 'INFO', 'Opening metadata store.' );

	eval {
		_log( 'INFO', 'Connecting to metadata store.' );
		my $dbh = DBI->connect( $config->{DatabaseConnect}, $config->{DatabaseUsername}, $config->{DatabasePassword}, { RaiseError => 1 } );

		$config->{dbh} = $dbh;
		
		eval {
			# Try to select metadata store
			_log( 'INFO', 'Checking we can read from metadata store.' );
			$config->{dbh}->do( $config->{SQLPing} );
		};
		if ( $@ ) {
			# On error try to create
			_log( 'INFO', 'Failed opening metadata store. Will try to create table.' );
			eval {
				$config->{dbh}->do( $config->{SQLCreateTable} );
			};
			if ( $@ ) {
				# Die if metadata is innaccessible and we cannot create store
				_logdie( 'ERROR', 'Could not open or create metadata store.' );
			}
		}
	};
	if ( $@ ) {
		# Could not connect
		_logdie( 'ERROR', 'Could not connect to metadata store.' );
	}
}

sub close_metadata_store( $ ) {
	my ( $config ) = @_;
	
	_log( 'INFO', 'Closing metadata store.');

	$config->{dbh}->disconnect;
}

sub cleanup( $ ) {
	my ( $config ) = @_;

	my $glacier = Net::Amazon::Glacier->new(
		$config->{VaultRegion},
		$config->{AWSAccessKey},
		$config->{AWSSecret}
	);

	# Checks vault exists, logs and dies if not as per specs.
	_check_vault( $glacier, $config );
	
	_log( 'INFO', 'Checking monthly exist before cleanup for monthly $config->{MonthlyCode}.');

	my $sth = $config->{dbh}->prepare( $config->{SQLSelectMonthly} );
	_log( 'INFO', "About to execute $config->{SQLSelectMonthly} with params $config->{MonthlyCode}");
	$sth->execute( $config->{MonthlyCode} );

	my $monthly = $sth->fetchrow_hashref();

	if ( $monthly->{archive_id} ) {
		_log( 'INFO', 'Monthly ok. Stored in Glacier as $monthly->{archive_id}.');
		my $sth_old_dailies = $config->{dbh}->prepare( $config->{SQLSelectDailies} );
		_log( 'INFO', "About to execute $config->{SQLSelectDailies} with params $config->{MonthlyCode}");
		my $will_delete_dailies = $sth_old_dailies->execute( $config->{MonthlyCode} );
		
		if ( $will_delete_dailies eq '0E0' ) {
			_log( 'WARN', "No dailies for month $config->{MonthlyCode}. Already clean?" );
		} else {
			_log( 'INFO', "I will delete $will_delete_dailies dailies for month $config->{MonthlyCode}" );
		}
		
		my ( $cleaned_dailies ) = 0;
		
		while ( my $old_daily = $sth_old_dailies->fetchrow_hashref() ) {
			_log( 'INFO', "Will cleanup daily $old_daily->{daily} with associated archive $old_daily->{archive_id}.");
			$config->{ArchiveId} = $old_daily->{archive_id};
			
			my $deleted = 0;
			eval {
				_delete( $config );
				$deleted = 1;
			};
			if ( $@ ) {
				_log( 'WARN', "Could not delete daily $old_daily->{daily} with associated archive $old_daily->{archive_id} from Glacier with error $@." );
			}
			
			if ( $deleted ) {
				_log( 'INFO', "SUCCESS, deleted daily $old_daily->{daily} with associated archive $old_daily->{archive_id} from Glacier!" );
				$cleaned_dailies++;
			} else {
				_log( 'WARN', "Could not deleted daily $old_daily->{daily} with associated archive $old_daily->{archive_id} from Glacier." );
			}			
		
			my $rows_deleted = $config->{dbh}->do( $config->{SQLDeleteSingleArchive}, undef, $old_daily->{archive_id} );
			if ( $rows_deleted == 1 ) {
				_log( 'INFO', "SUCCESS, deleted daily $old_daily->{daily} with associated archive $old_daily->{archive_id} from metadata!" );
			} else {
				_log( 'WARN', "Could not deleted daily $old_daily->{daily} with associated archive $old_daily->{archive_id} from metadata." );
			}
		}
		if ( $cleaned_dailies == $will_delete_dailies ) {
			_logemail( 'INFO', "SUCCESS, deleted the expected number of dailies for $config->{MonthlyCode}." );
		} else {
			_logemail( 'WARN', "Number of dailies mismatch in cleanup for $config->{MonthlyCode}." );
		}
	} else {
		_log( 'WARN', 'No monthly for given month. Already clean?' );
	}
}

sub _delete($) {
	my ( $config ) = @_;
	
	_log( 'INFO', "Calling Glacier delete_archive for $config->{Vault}, $config->{ArchiveId}");

	my $glacier = Net::Amazon::Glacier->new(
		$config->{VaultRegion},
		$config->{AWSAccessKey},
		$config->{AWSSecret}
	);

	return $glacier->delete_archive( $config->{Vault}, $config->{ArchiveId} );
}

sub _backup($) {
	my ( $config ) = @_;
	
	my $glacier = Net::Amazon::Glacier->new(
		$config->{VaultRegion},
		$config->{AWSAccessKey},
		$config->{AWSSecret}
	);
	
	_check_archive_exists( $config );
	
	# Checks vault exists, logs and dies if not as per specs.
	_check_vault( $glacier, $config );

	my $retry = 0;
	my $archive_id = undef;

	while ( $retry++ <= $config->{RetryBeforeError} && !defined $archive_id ) {
		_log( 'INFO', "This is try $retry on this set.");
		
		my $file_size = 0;
		# Estimate part size for estimated tarred directory size
		my $tarred_size = _tar_files_size( $config->{backup_files} );
		_log( 'INFO', "Estimated tar size to $tarred_size.");
		my $part_size = $glacier->calculate_multipart_upload_partsize( $tarred_size );
		_log( 'INFO', "Estimated Glacier part_size to $part_size.");
		
		my $current_upload_id;
		eval {
			$current_upload_id = $glacier->multipart_upload_init( $config->{VaultName}, $part_size, $config->{Comment} ) unless $config->{NoGlacierAPICalls};
		};
		if ( $@ ) {
			_log( 'ERROR', "Could not initialize multipart_upload. This is retry $retry of $config->{RetryBeforeError}. Glacier returned error: $@.");
		} else {
			_log( 'INFO', "SUCCESS, UploadId $current_upload_id returned from Glacier, multipart started.");
		}
		
		if ( $current_upload_id ) {
			my $part_index = 0;
			my $parts_hash = [];
	
			# Cleanup only when not debugging
			my $dir = File::Temp::tempdir( CLEANUP => ( $config->{Debug}?0:1 ) );
	
			tie *TAR, 'Tie::FileHandle::Split', $dir, $part_size;
	
			_log( 'INFO', 'Setting up tar splitting.');
			# Gets called when a part is generated by the streaming tar
			(tied *TAR)->add_file_creation_listeners( sub {
				my ( $tied_filehandle, $current_part_temp_path  ) = @_;
	
				# Compute last file size, most will be part_size, last might not
				$file_size += -s $current_part_temp_path;
				
				_log( 'INFO', "Part size if " . ( -s $current_part_temp_path ) . " from $current_part_temp_path" );
				
				_log( 'INFO', "A part of the archive has been stored and will be uploaded. Vault: $config->{VaultName}, UploadId: $current_upload_id PartSize: $part_size, PartIndex: $part_index, TempPath: $current_part_temp_path");
				
				my $retry_part = 0;
				
				$parts_hash->[$part_index]='';
				
				while ( $retry_part++ <= $config->{RetryBeforeError} && $parts_hash->[$part_index] !~ /^[0-9a-f]{64}$/ ) {
					_log( 'INFO', "This is my try $retry_part on part $part_index.");
					
					eval {
						$parts_hash->[$part_index] = $glacier->multipart_upload_upload_part( $config->{VaultName}, $current_upload_id, $part_size, $part_index, $current_part_temp_path ) unless $config->{NoGlacierAPICalls};
					};
					if ( $@ ) {
						_log( 'WARN', "Uploading part to Glacier failed with error $@. Vault: $config->{VaultName}, UploadId: $current_upload_id PartSize: $part_size, PartIndex: $part_index, TempPath: $current_part_temp_path");
					} else {
						_log( 'INFO', "SUCCESS, uploading part to Glacier completed. Vault: $config->{VaultName}, UploadId: $current_upload_id PartSize: $part_size, PartIndex: $part_index, TempPath: $current_part_temp_path, PartHash: $parts_hash->[$part_index]");
					}
					
					if( $parts_hash->[$part_index] !~ /^[0-9a-f]{64}$/ ) {
						unless ( $config->{NoGlacierAPICalls} ) {
							_log( 'ERROR', 'Not a valid part hash returned from Glacier: $parts_hash->[$part_index].');
						}
					} else {
						# Dispose temp file when not debugging
						unlink $current_part_temp_path unless $config->{Debug};
					}
				}
				
				if ( $retry_part++ > $config->{RetryBeforeError} ) {
					if ( $config->{Daily} ) {
						_logemail( 'WARN', "Retrying a part more than $config->{RetryBeforeError} times for a single part on daily $config->{DailyCode}." );
					} elsif( $config->{Monthly} ) {
						_logemail( 'WARN', "Retrying a part more than $config->{RetryBeforeError} times for a single part on monthly $config->{MonthlyCode}." );
					}
				}
				
				if( $parts_hash->[$part_index] !~ /^[0-9a-f]{64}$/ ) {
					# Go to next part index next time
					$part_index++;
					$parts_hash->[$part_index]='';
				}
			});
	
			_log( 'INFO', "Will begin tarring files.");
			# This calls the listener for each part
			_tar_files_contents( \*TAR, $config->{backup_files} );
	
			# This could call the listener for the last part
			(tied *TAR)->write_buffers();
	
			_log( 'INFO', "Will try to complete upload to Glacier.");
			#$archive_id undef unless completed
			eval {
				$archive_id = $glacier->multipart_upload_complete( $config->{VaultName}, $current_upload_id, $parts_hash, $file_size ) unless $config->{NoGlacierAPICalls};
			};
			if ( $@ ) {
				_log( 'WARN', $@ );
			}
			if ( defined $archive_id ) {
				_log( 'INFO', "Glacier accepted archive $archive_id will try to store in metadata.");
				eval {
					my $sth = $config->{dbh}->prepare( $config->{SQLInsertSingleArchive} );
					$sth->execute(
						$archive_id,
						$config->{Comment},
						DateTime->now->ymd . ' ' . DateTime->now->hms,
						$config->{DailyCode},
						$config->{MonthlyCode},
					) unless $config->{NoGlacierAPICalls};
					_log( 'INFO', "SUCCESS, stored to Glacier and metadata with archive_id: $archive_id");
				};
				if ( $@ ) {
					_log( 'ERROR', "Could not store metadata for $archive_id with error: $@.");
				}
			} else {
				_log( 'ERROR', "Bad archive_id generated. Glacier returned error: $@.") unless $config->{NoGlacierAPICalls} || defined $archive_id;
			}
		} # Skips unless UploadId created successfully.
	} # Retryies until $config->{RetryBeforeError} or an archive_id is generated
	
	if ( $archive_id ) {
		_logemail( 'INFO', "SUCCESS, uploaded archive with archive_id $archive_id.");
	} else {
		#also $retry > $config->{RetryBeforeError} {
		_logemail( 'ERROR', "Failed to upload archive after $config->{RetryBeforeError}.");
	}

	return $archive_id;
}

sub _check_vault($$) {
	my ( $glacier, $config ) = @_;
	
	_log( 'INFO', "Will check vault '$config->{VaultName}' exists and is describable.");
	
	# Check Vault exists
	_logdie( 'ERROR', "Describe_vault failed for $config->{VaultName}. Check AWS Credentials, vault config and credentials permissions.")
		unless $config->{NoGlacierAPICalls} || eval {
			$glacier->describe_vault( $config->{VaultName} );
			1;
	};
}

sub _check_archive_exists($) {
	my ( $config ) = @_;
	my $rows;
	
	_log( 'INFO', "Will check if archive already exists." );
	
	my ( @row );
	
	if ( $config->{Daily} ) {
		@row = $config->{dbh}->selectrow_array( $config->{SQLExistsDaily}, undef, $config->{DailyCode} );
	} elsif ( $config->{Monthly} ) {
		@row = $config->{dbh}->selectrow_array( $config->{SQLExistsDaily}, undef, $config->{MonthlyCode} );
	} else {
		_logemail( 'ERROR', 'Unexpected timelapse trying to backup. I know Daily and Monthly backups' );
	}

	_logdie( 'ERROR', 'Archive already exists trying to run backup!') if ( defined $row[1] );
	
	return ( defined $row[1] );
}

# Split input from file handle into n part_size files
sub _store_file_part() {
	my ( $config, $fh, $part_size ) = @_;

	my ($temp_fh, $temp_name) = File::Temp::mkstemp( );

	binmode $temp_fh;

	my $at = 0;
	my $buf;

	# Read parts as long a reading a part will not get us past a part_limit
	# or until eof
	while ( $at + $config->{ReadBufferSize} <= $part_size && !$fh->eof ) {
		read( $fh, $buf, $config->{ReadBufferSize} );
		$temp_fh->print( $buf );
		$at += $config->{ReadBufferSize};
	}

	unless ( $fh->eof ) {
		# Calculate remaing bytes to read until part_size in case ReadBufferSize does
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
	# Separate and testable
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

sub _tar_files_size ($) {
	# File path are somehow limited. If path are to long additional info blocks might be generated by tar, since we are streaming we could get into trouble with maximum file pieces
	my ( $dirs ) = @_;

	my $size_sum = 0; # adds metadata blocks and archive content size with proper rounding

	# -s returns 0 for directories, which is consistent
	foreach my $file ( @$dirs ) {
		$size_sum += _tar_archive_member_size( -s $file) unless (-d $file);
	}
	
	return _tar_output_file_size( $size_sum );
}

sub _tar_files_contents ($$) {
	my ( $fh, $dirs ) = @_;

  my $tar = Archive::Tar::Streamed->new( $fh );
	my $file;
	
	foreach my $file ( @$dirs ) {
		_log( 'INFO', "Add $file to archive." );
		$tar->add( $file );
	}
}

sub _get_local_file_list($$) {
	my ( $dir, $date_path ) = @_;
	my ( @filelist );

	my $path = quotemeta File::Spec->catpath( '', $dir, $date_path );
	
	_log( 'INFO', "Adding files from $path to backup set." );
	File::Find::find( {
		no_chdir => 1,
		wanted => sub {
			#all non-directory that match path beginning
			push @filelist, $File::Find::name if ( defined $File::Find::name && !(-d $File::Find::name) && $File::Find::name =~ /^${path}/ );
			}
	}, $dir );
	return \@filelist;
}

sub _log($$) {
	my ( $level, $message ) = @_;
	
	print "$level, $message\n";
}

sub _logemail($$) {
	my ( $level, $message ) = @_;
	
	_log( $level, "THIS WILL BE AN MAIL: $message" );
}

sub _logfinishandemail($$) {
	my ( $level, $message ) = @_;
	
	_log( $level, "THIS WILL BE AN MAIL: $message" );
}

sub _logdie($$) {
	my ( $level, $message ) = @_;
	_log( $level, $message );
	die( $message );
}

1;
