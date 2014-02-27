#!/usr/bin/perl -w
use strict;
use Test::More tests=>30;
use Carp;

use lib '../..';

BEGIN {
	# Be sure we can use all required modules for BackupImageCacher
	use_ok( 'HomeCo::AWS::BackupImageCacher' );
	use_ok( 'Net::Amazon::Glacier', 0.13 );
	use_ok( 'Local::AWS::Credentials' );
	use_ok( 'File::Temp' );
	use_ok( 'File::Find' );
	use_ok( 'File::Spec' );
	use_ok( 'File::Temp' );
	use_ok( 'DateTime' );
	use_ok( 'Carp' );
}

# A wierd name for test vaults
my $test_vault_name = 'test_backupimage_cacher_x41925493445';

# Expected methods for BackupImageCacher and Glacier
my @methods_backup = qw( backup check_parameters parameter_match open_matadata_store ping_metadata_store close_matadata_store cleanup );
my @methods_glacier = qw( create_vault delete_vault describe_vault list_vaults set_vault_notifications get_vault_notifications
delete_vault_notifications upload_archive delete_archive initiate_archive_retrival initiate_inventory_retrieval
initiate_job describe_job get_job_output list_jobs calculate_multipart_upload_partsize
);

# Read credentials from file
my $credential_file_path = '../../.aws_credentials.txt';
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
print 'HomeCo::AWS::BackupImageCacher expected methods exist'
	if can_ok( 'HomeCo::AWS::BackupImageCacher', @methods_backup );
print 'Net::AWS::Glacer expected methods exist'
	if can_ok( $glacier, @methods_glacier );
# Check credentials are coherent
ok ( length( $aws_access_key ) > 0, 'Minimally coherent AWS access key');
ok ( length( $aws_secret_key ) > 0, 'Minimally coherent AWS secret');
# Check credentials against AWS with dummy request
my $vaults = $glacier->list_vaults();
is ( ref $vaults, 'ARRAY', 'list_vaults(): Returns an array of vaults, keys are valid' );
# Test for expected Glacier exceptions
&test_resource_not_found_exception;
# Test calculating tar member sizes
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 0 ), 1536, 'zero length files tar member size calculated ok = 3 meta + 0 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 1 ), 2048, 'single byte files tar member size calculated ok = 3 meta + 1 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 511 ), 2048, 'block minus one byte files tar member size calculated ok = 3 meta + 1 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 512 ), 2048, 'single record files tar member size calculated ok = 3 meta + 1 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 513 ), 2560, 'single record + 1 byte files tar member size calculated ok = 3 meta + 2 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 1024 ), 2560, 'two record files tar member size calculated ok = 3 meta + 2 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 10240 ), 11776, 'blocking factor sized tar member size calculated ok = 3 meta + 10 content');
is( HomeCo::AWS::BackupImageCacher::_tar_archive_member_size( 10737418240 ), 10737419776, '10 GiB tar member size calculated ok = 3 meta + 20971520 content');
# Test calculating tar file sizes
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 0 ), 10240, '0 sized output file tar output size calculated ok = 10 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 10240 ), 10240, '10 block sized output file tar output size calculated ok = 10 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 10241 ), 20480, '10 block + 1 byte sized output file tar output size calculated ok = 20 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 20480 ), 20480, '20 block sized output file tar output size calculated ok = 20 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 107374182400 ), 107374182400, '100 GiB sized output file tar output size calculated ok = 104857600 blocks');
is( HomeCo::AWS::BackupImageCacher::_tar_output_file_size( 4294967296000 ), 4294967296000, '4000 GiB sized output file tar output size calculated ok = 419430400 blocks');

#
# Helper methods
#
sub get_random_test_vault_name($) {
	my $test_vault_prefix = $_;
	my $random_vault = $test_vault_prefix;
	$random_vault .=  rand 9 for 1..10; # a long string of numbers

	return $random_vault;
}

# Generates a directory with a set of files for testing
sub generate_files {
	my @file_sizes = @_;

	my $temp_dir = File::Temp::tempdir->new();
	my $temp_files = {};

	foreach my $file_size ( @file_sizes ) {
		#test simple upload of generated files
		try {
			my ( $temp_fh, $temp_filename );
			( $temp_fh, $temp_filename ) = File::Temp::tempfile( DIR=> $temp_dir );
			$temp_files->{ $temp_filename } = $temp_fh;
		} catch {
			clean_upfiles( $temp_files, $temp_dir );
			BAIL_OUT( 'Could not create files for testing' );
		};

		#test generating content for files for tarring
		try {
			foreach my $file_fh ( values $temp_files ) {
				binmode $file_fh;

				$file_fh->print( get_random_file_content( $file_size ) );
			}
		} catch {
			clean_upfiles( $temp_files, $temp_dir );
			BAIL_OUT( 'Could not generate content for etst files' )
		};
	}
}

sub get_random_file_content($) {
	my $content_length = $_;

	croak "$content_length not a valid number in get_random_file_content" if ( $content_length > 0 );

	my $random_content .= chr (rand 255) for ( 1 .. $content_length );
}

sub cleanup_files ($$) {
	my ( $files, $temp_dir ) = @_;

	#try to clean up
	foreach my $file ( keys $files ) {
		unlink $file->filename;
	}
	rmdir $temp_dir;
}

#
# Complex tests as methods
#

sub test_resource_not_found_exception {
	eval {
		my $exception;
		eval {
			my $job = $glacier->describe_job( 'non_existent_5234729563454','not_a_job_14890624' );
		};
		if ( $@ ) {
			$exception = $@;
		}
		like ( $exception, qr/(ResourceNotFoundException)/, 'describe_job(): ResourceNotFoundException correctly reported.');
	};
	if ( $@ ) {
		BAIL_OUT( 'Unknown error testing for Glacier describe_job exception handling for ResourceNotFoundException.' );
	}
}
