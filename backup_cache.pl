use strict;
use warnings;
use Config::Simple;
use Getopt::Long;
use DateTime;
use Local::AWS::Credentials;
use HomeCo::AWS::BackupImageCacher;

# Calculate config file name
my $config = {}; #must be empty hash for Config::Simple
my $config_file = $0; $config_file =~ s/\.([^\.]+)$/\.conf/;

# Check we have a config file with the expected name
HomeCo::AWS::BackupImageCacher::_logdie( 'ERROR', "No config file $config_file!" ) unless -f $config_file;

# Default values
$config->{Daily} = '';
$config->{Monthly} = '';
$config->{Date} = DateTime->now->set_time_zone('local')->subtract( days => 1 )->ymd('');

# Read config file
Config::Simple->import_from( $config_file, $config );

# Merge command line options with config file, command line has precedense
HomeCo::AWS::BackupImageCacher::_logdie( 'ERROR', "Failed reading command line parameters." )
	unless GetOptions(
		HomeCo::AWS::BackupImageCacher::parameter_match( $config ),
);

eval {
	# Read credentials
	( $config->{AWSAccessKey}, $config->{AWSSecret} ) = Local::AWS::Credentials::read_aws_credentials( $config->{AWSCredentials} );
};
if ( $@ ) {
	HomeCo::AWS::BackupImageCacher::_logdie( 'ERROR', "Failed reading credentials with error: $@." );
}

# Request parameter check
eval {
	HomeCo::AWS::BackupImageCacher::check_parameters( $config );
};
if ( $@ ) {
	HomeCo::AWS::BackupImageCacher::_logdie( 'ERROR', "Parameter check failed with error: $@." );
}
eval {
	HomeCo::AWS::BackupImageCacher::open_metadata_store( $config );
};
if ( $@ ) {
	HomeCo::AWS::BackupImageCacher::_logdie( 'ERROR', "Opening metadata store failed with error: $@." );
}

if ( $config->{Daily} || $config->{Monthly} ) {
	eval {
		HomeCo::AWS::BackupImageCacher::backup( $config );
	};
	if ( $@ ) {
		HomeCo::AWS::BackupImageCacher::_logdie( 'ERROR', "Backup failed with error: $@." );
	}
} elsif ( $config->{Cleanup} ) {
	eval {
		HomeCo::AWS::BackupImageCacher::cleanup( $config );	
	};
	if ( $@ ) {
		HomeCo::AWS::BackupImageCacher::_logdie( 'ERROR', "Cleanup failed with error: $@." );
	}
} else {
	HomeCo::AWS::BackupImageCacher::_logdie( 'ERROR', "Inconsistent parameters. Daily, Monthly or Cleanup must be specified." );
}

eval {
	HomeCo::AWS::BackupImageCacher::close_metadata_store( $config );
};
if ( $@ ) {
	HomeCo::AWS::BackupImageCacher::_log( 'INFO', "Could not close metadata store with error: $@" );
}
