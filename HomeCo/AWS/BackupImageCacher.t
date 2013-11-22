#!/usr/bin/perl -w
use strict;
use Test::More tests=>3;

$test_vault_prefix = 'test_backupimage_cacher_x41925493445_';

#expected methods
@methods_backup = qw( backup_daily backup_monthly );
@methods_glacier = qw(
create_vault delete_vault describe_vault list_vaults set_vault_notifications get_vault_notifications delete_vault_notifications
upload_archive delete_archive
initiate_archive_retrival initiate_invenrory_retrieval initiate_job describe_job get_job_output list_jobs
calculate_multipart_upload_partsize
);

#test the module can be added
use_ok( 'HomeCo::AWS::BackupImageCacher' );
use HomeCo::AWS::BackupImageCacher;
use_ok( 'Net::AWS::Glacier', 0.13 );
use Net::AWS::Glacier 0.13;
use_ok( 'Local::AWS::Credentials' );
use Local::AWS::Credentials;
use_ok( 'File::Temp' );
use File::Temp;
use_ok( 'Carp' );
use Carp;

#instance objects
$backup_imager = new HomeCo::AWS::BackupImageCacher;
$glacier = new Net::AWS::Glacier;

#check the object are blessed
isa_ok( $backup_imager, 'HomeCo::AWS::BackupImageCacher' );
isa_ok( $glacier, 'Net::AWS::Glacier' );

#check all expected methods exist
can_ok( $backup_imager, @methods_backup, 'HomeCo::AWS::BackupImageCacher expected methods exist' );
can_ok( $glacier, @methods_glacier, 'Net::AWS::Glacer expected methods exist' );

#read credentials from file
my ( $aws_access_key, $aws_secret_key ) = Local::AWS::Credentials::read_aws_credentials( $credential_file_path  );

#check credentials are coherent
is ( length( $aws_access_key ) > 0, 'Coherent AWS access key');
is ( length( $aws_secret_key ) > 0, 'Coherent AWS secret');

#check credentials against AWS with dummy request
$vaults = $glacier->list_vaults();
is ( ref $vaults, 'ARRAY', 'list_vaults(): Returns an array of vaults, keys are valid' );

#test for exceptions
try {
	my $exception;
	try {
		$job = $glacier->describe_job( 'non_existent_5234729563454','not_a_job_14890624' );
	} catch ( $exception = $_ );

	like ( $exception, qr/describe_job failed/, 'describe_job(): Expected exception reported');
} catch ( BAIL_OUT( 'Unknown error testing for Glacier describe_job exception handling') );

#check no test vaults exist or reset test environment
foreach ( my $vault ( $@vaults) ) {
	#delete vaults beginning with test string
	if ( $vault qr/^$test_vault_prefix/ ) {
		try {
			$glacier->delete_vault( $vault );
		} catch ( warn( "Could not reset test environment" ) );
	}
}

#check for files in test vaults

#test calculating tar file size

sub get_random_test_vault_name() {
	my $random_vault = $test_vault_name;
	$random_vault .=  rand 9 for 1..10; # a long string of numbers

	return $random_vault;
}

#generate a directory with a set of files
sub generate_files () {
	my @file_sizes = @_;

	my $temp_dir = File::Temp::tempdir->new();
	my $temp_files = {};

	foreach my $file_size ( @file_sizes ) {
		#test simple upload of generated files
		try {
			( $temp_file->fh, $temp_file->filename ) = File::Temp::tempfile( DIR=> $temp_dir );
		} catch {
			clean_upfiles( $temp_file );
			BAIL_OUT( 'Could not create files for testing' );
		};

		#test generating content for files for tarring
		try {
			foreach my $file_fh ( @temp_file_fhs ) {
				binmode $file_fh;

				$file_fh->print( get_random_file_content( (rand 10000) + 1 ) );
			}
		} catch {
			clean_upfiles( $temp_file );
			BAIL_OUT( 'Could not generate content for etst files' )
		};
	}
}

sub get_random_file_content () {
	my $content_length = $_;

	crock "$content_length not a valid number in get_random_file_content" if ( $content_length > 0 );

	my $random_content .= chr (rand 255) for
}

sub cleanup_files () {
	my $files = $_;

	#try to clean up
	foreach my $file ( keys temp_file ) {
		unlink $file->filename;
	}
	rmdir $temp_dir;
}
