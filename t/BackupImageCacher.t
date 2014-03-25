#!/usr/bin/perl -w
use strict;
use Test::More tests=>80;
use Carp;

use lib '..';

# Perform only local tests, no connection to Glacier
my $only_local = 1;

BEGIN {
	# Be sure we can use all required modules for BackupImageCacher
	use_ok( 'HomeCo::AWS::BackupImageCacher' );
	use_ok( 'Net::Amazon::Glacier', 0.14 );
	use_ok( 'Local::AWS::Credentials' );
	use_ok( 'File::Temp' );
	use_ok( 'File::Find' );
	use_ok( 'File::Spec' );
	use_ok( 'DateTime' );
	use_ok( 'Carp' );
	use_ok( 'DBI' );
	use_ok( 'Archive::Tar::Streamed' );
	use_ok( 'Tie::FileHandle::Split' );
}

# A wierd name for test vaults
my $test_vault_name = 'test_backupimage_cacher_x41925493445';

# Expected methods for BackupImageCacher and Glacier
my @methods_backup = qw( backup check_parameters parameter_match open_metadata_store ping_metadata_store close_metadata_store cleanup );
my @methods_glacier = qw( create_vault delete_vault describe_vault list_vaults set_vault_notifications get_vault_notifications
delete_vault_notifications upload_archive delete_archive initiate_inventory_retrieval
initiate_job describe_job get_job_output list_jobs calculate_multipart_upload_partsize
);
#check later initiate_archive_retrival

# Read credentials from file
my $credential_file_path = '../.aws_credentials.txt';
my ( $aws_access_key, $aws_secret_key ) = Local::AWS::Credentials::read_aws_credentials( $credential_file_path  );
# Instanciate objects
my $glacier = Net::Amazon::Glacier->new(
	'us-west-1',
	$aws_access_key,
	$aws_secret_key
);
# Check Net::Amazon::Glacier objects are blessed correctly
isa_ok( $glacier, 'Net::Amazon::Glacier' );
# Check expected methods exist
can_ok( 'HomeCo::AWS::BackupImageCacher', @methods_backup );
can_ok( $glacier, @methods_glacier );
# Check credentials are coherent
ok ( length( $aws_access_key ) > 0, 'Minimally coherent AWS access key');
ok ( length( $aws_secret_key ) > 0, 'Minimally coherent AWS secret');
# Check credentials against AWS with dummy request
SKIP: {
  skip 'No connection tests will be performed', 2 if $only_local;

	my $vaults = $glacier->list_vaults();
	is ( ref $vaults, 'ARRAY', 'list_vaults(): Returns an array of vaults, keys are valid' );
	# Test for expected Glacier exceptions
	&test_resource_not_found_exception;
}
#
# Valid parameters tests
#
my $parameters = {};
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/BaseThumbs not specified./,"PARAM BaseThumbs defined checked.");
$parameters->{BaseThumbs} = '*'; # Should not exist
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/BaseThumbs does not exist./, "PARAM BaseThumbs exist checked.");
$parameters->{BaseThumbs} = '.'; # Exists most of the time!
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/BaseImageCache not specified./, "PARAM BaseImageCache defined checked.");
$parameters->{BaseImageCache} = '*';
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/BaseImageCache does not exist./, "PARAM BaseImageCache exist checked.");
$parameters->{BaseImageCache} = '.';
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/VaultRegion not specified./, "PARAM VaultRegion specified checked.");
$parameters->{VaultRegion} = 'us-east-1';
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/VaultName not specified./, "PARAM VaultName specified checked.");
$parameters->{VaultName} = 'test';
$parameters->{AWSCredentials} = '*';
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/AWSCredentials file does not exist./, "PARAM AWSCredentials file exists checked.");
$parameters->{AWSCredentials} = '.';
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/Either daily, monthly or cleanup must be specified./, "PARAM Checks request is specified.");
$parameters->{Daily} = 1; $parameters->{Monthly} = 1;
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/Cannot request daily and monthly backup in a single run./, "PARAM Checks requests do not overlap.");
$parameters->{Daily} = 1; $parameters->{Monthly} = 0;
$parameters->{Date} = '20140132'; # Invalid date, beyond month range
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/Provided date is invalid/, "PARAM Checks invalid dates on simple ranges.");
$parameters->{Date} = '19000229'; # Invaliddate, non leap year, mod 100
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/Provided date is invalid/, "PARAM Checks invalid dates on complex leap years.");
$parameters->{Date} = '20141301'; # Invalid date, valid when month/day order is changed
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/Provided date is invalid/, "PARAM Checks invalid dates with locales mismatches.");
$parameters->{Date} = '20140305'; # Valid date
exception_like( \&HomeCo::AWS::BackupImageCacher::check_parameters, $parameters, qr/Thumbs directory does not exists at/, "PARAM Checks valid dates that errored to avoid regression.");

#
# TAR tests
#

# TAR members tests
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 0 ), 1536, 'TAR zero length files tar member size calculated ok = 3 meta + 0 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 1 ), 2048, 'TAR single byte files tar member size calculated ok = 3 meta + 1 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 511 ), 2048, 'TAR block minus one byte files tar member size calculated ok = 3 meta + 1 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 512 ), 2048, 'TAR single record files tar member size calculated ok = 3 meta + 1 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 513 ), 2560, 'TAR single record + 1 byte files tar member size calculated ok = 3 meta + 2 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 1024 ), 2560, 'TAR two record files tar member size calculated ok = 3 meta + 2 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 10240 ), 11776, 'TAR blocking factor sized tar member size calculated ok = 3 meta + 10 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 10737418240 ), 10737419776, 'TAR 10GiB tar member size calculated ok = 3 meta + 20971520 content');
# TAR single file size tests
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 0 ), 10240, 'TAR 0 sized output file tar output size calculated ok = 1 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 10240 ), 10240, 'TAR 1 block sized output file tar output size calculated ok = 1 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 10241 ), 20480, 'TAR 1 block + 1B sized output file tar output size calculated ok = 2 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 20480 ), 20480, 'TAR 2 block sized output file tar output size calculated ok = 2 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 30719 ), 30720, 'TAR 3 - 1bib block sized output file tar output size calculated ok = 3 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 30720 ), 30720, 'TAR 3 block sized output file tar output size calculated ok = 3 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 30721 ), 40960, 'TAR 4 block sized output file tar output size calculated ok = 4 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 107374182400 ), 107374182400, 'TAR 100 GiB sized output file tar output size calculated ok = 10485760 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 4294967296000 ), 4294967296000, 'TAR 4000 GiB sized output file tar output size calculated ok = 41943040 blocks');
# TAR multiple file size tests
&test_directory_with_files_of_size( [ 0 ], 10240, 'TAR single 0B = 1 block' );
&test_directory_with_files_of_size( [ 511 ], 10240, 'TAR single 511B = 1 block' );
&test_directory_with_files_of_size( [ 0, 1 ], 10240, 'TAR single 0B, 1B = 1 block' );
&test_directory_with_files_of_size( [ 0, 511 ], 10240, 'TAR 0B, 511B = 1 block' );
&test_directory_with_files_of_size( [ 0, 512 ], 10240, 'TAR 0B, 512B = 1 block' );
&test_directory_with_files_of_size( [ 0, 513 ], 10240, 'TAR 0B, 513B = 1 block' );
&test_directory_with_files_of_size( [ 0, 7167 ], 10240, 'TAR 0B, 1 block - 2 *3*512B sized file - 1B = 1 block' );
&test_directory_with_files_of_size( [ 0, 7168 ], 10240, 'TAR 0B, 1 block - 2 *3*512B sized file = 1 block' );
&test_directory_with_files_of_size( [ 0, 7169 ], 20480, 'TAR 0B, 1 block - 2 *3*512B sized file + 1B = 2 block' );
&test_directory_with_files_of_size( [ 8703 ], 10240, 'TAR 1 block - 3 * 512B - 1B = 1 block' );
&test_directory_with_files_of_size( [ 8704 ], 10240, 'TAR 1 block - 3 * 512B = 1 block' );
&test_directory_with_files_of_size( [ 8705 ], 20480, 'TAR 1 block - 3 * 512B + 1B = 2 block' );
&test_directory_with_files_of_size( [ 0, 8703 ], 20480, 'TAR 0bib, 1 block - 3 * 512B - 1B = 2 blocks' );
&test_directory_with_files_of_size( [ 0, 8704 ], 20480, 'TAR 0bib, 1 block - 3 * 512B = 2 blocks' );
&test_directory_with_files_of_size( [ 0, 8705 ], 20480, 'TAR 0bib, 1 block - 3 * 512B + 1B = 2 blocks' );
&test_directory_with_files_of_size( [ 0, 10240, 5631 ], 20480, 'TAR 0B, 1 block, 1 block - 3*3*512B - 1B = 2 blocks' );
&test_directory_with_files_of_size( [ 0, 10240, 5632 ], 20480, 'TAR 0B, 1 block, 1 block - 3*3*512B = 2 blocks' );
&test_directory_with_files_of_size( [ 0, 10240, 5633 ], 30720, 'TAR 0B, 1 block, 1 block - 3*3*512B + 1B = 3 blocks' );
&test_directory_with_files_of_size( [ ( 8704 ) x 2, 8703 ], 30720, 'TAR 2 * ( 1 block - 3 * 512B ), 1 block - 3 * 512B - 1 B = 3 block' );
&test_directory_with_files_of_size( [ ( 8704 ) x 3 ], 30720, 'TAR 3 * ( 1 block - 3 * 512B ) = 3 block' );
&test_directory_with_files_of_size( [ ( 8704 ) x 2, 8705], 40960, 'TAR 2 * ( 1 block - 3 * 512B ), 1 block - 3 * 512B + 1 B = 4 block' );
&test_directory_with_files_of_size( [ ( 8704 ) x 3, 8703 ], 40960, 'TAR 3 * ( 1 block - 3*3*512B ), 1 block - 3*3*512B - 1 = 4 blocks' );
&test_directory_with_files_of_size( [ ( 8704 ) x 4 ], 40960, 'TAR 4 * ( 1 block - 3*3*512B ) = 4 blocks' );
&test_directory_with_files_of_size( [ ( 8704 ) x 3, 8705 ], 51200, 'TAR 3 * ( 1 block - 3*3*512B ), 1 block - 3*3*512B + 1 = 5 blocks' );
&test_directory_with_files_of_size( [ ( 512 ) x 4, 511 ], 10240, 'TAR 4 * ( 512B ), 511B = 1 block' );
&test_directory_with_files_of_size( [ ( 512 ) x 5 ], 10240, 'TAR 5 * ( 3*512B + 512B ) = 1 block' );
&test_directory_with_files_of_size( [ ( 512 ) x 4, 513 ], 20480, 'TAR 4 * ( 3*512B + 512B ), 3*512B + 513B = 2 blocks' );
&test_directory_with_files_of_size( [ ( 0 ) x 6 ], 10240, 'TAR 6 0B files = 1 block' );
&test_directory_with_files_of_size( [ ( 0 ) x 7 ], 20480, 'TAR 7 0B files = 2 blocks' );
&test_directory_with_files_of_size( [ ( 0 ) x 8 ], 20480, 'TAR 8 0B files = 2 blocks' );
&test_directory_with_files_of_size( [ ( 0 ) x 19 ], 30720, 'TAR 19 0B files = 3 blocks' );
&test_directory_with_files_of_size( [ ( 0 ) x 20 ], 30720, 'TAR 20 0B files = 3 blocks' );
&test_directory_with_files_of_size( [ ( 0 ) x 21 ], 40960, 'TAR 21 0B files = 4 blocks' );

#
# Helper methods
#

# Generates a directory with a set of files for testing
sub generate_files {
	my $file_sizes_or_content = shift;

	my $temp_dir = File::Temp::tempdir();
	my $temp_files = {};

	foreach my $file_size_or_content ( @$file_sizes_or_content ) {
		#test simple upload of generated files
		eval {
			my ( $temp_fh, $temp_filename );
			( $temp_fh, $temp_filename ) = File::Temp::tempfile( DIR=> $temp_dir );
			$temp_files->{ $temp_filename } = $temp_fh;

			binmode $temp_fh;

			# If we get numbers we generate random content for that.
			# If we get content e use it for the file contents directly.
			my $data;
			if ( $file_size_or_content =~ /\d+/ ) {
				$data = get_random_file_content( $file_size_or_content );
			} else {
				$data = $file_size_or_content;
			}

			$temp_fh->print( $data ) if ( defined $data );

			$temp_fh->close();
		};
		if ( $@ )  {
			cleanup_files( $temp_files, $temp_dir );
			BAIL_OUT( 'Could not create files for testing' );
		};
	}

	return ( $temp_files, $temp_dir);
}

sub get_random_file_content($) {
	my $content_length = shift;

	croak "$content_length not a valid number in get_random_file_content" if ( $content_length < 0 );

	my $random_content;

	for ( 1 .. $content_length ) {
		$random_content .= chr ( int( rand 255 ) );
	}

	return $random_content;
}

sub cleanup_files ($$) {
	my ( $files, $temp_dir  ) = @_;

	#try to clean up
	foreach my $file ( keys %$files ) {
		unlink $file;
	}
	rmdir $temp_dir;
}

#
# Complex tests as methods
#

sub exception_like {
	my ( $code, $parameters, $like_this, $explanation ) = @_;

	eval {
		&$code( $parameters );
	};
	like( $@, $like_this, $explanation );
}

sub test_resource_not_found_exception() {
	eval {
		my $exception;
		eval {
			my $job = $glacier->describe_job( 'non_existent_5234729563454','not_a_job_14890624' );
		};
		like ( $@, qr/(ResourceNotFoundException)/, 'describe_job(): ResourceNotFoundException correctly reported.');
	};
	if ( $@ ) {
		BAIL_OUT( 'Unknown error testing for Glacier describe_job exception handling for ResourceNotFoundException.' );
	}
}

sub test_directory_with_files_of_size($$$) {
	my ( $file_sizes, $expected_size, $ok_msg ) = @_;

	my ( $temp_files, $temp_dir ) = &generate_files( $file_sizes );
	is( HomeCo::AWS::BackupImageCacher::_tar_directory_size( [ $temp_dir ] ), $expected_size, $ok_msg );
	&cleanup_files( $temp_files, $temp_dir );
}
