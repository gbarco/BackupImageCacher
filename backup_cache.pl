use strict;
use warnings;
use Config::Simple;
use Getopt::Long;
use DateTime;
use Local::AWS::Credentials;
use HomeCo::AWS::BackupImageCacher;

# calculate config file name
my $config = {}; #must be empty hash for Config::Simple
my $config_file = $0; $config_file =~ s/\.([^\.]+)$/\.cfg/;
die("No config file $config_file!") unless -f $config_file;

# default values
$config->{Daily} = '';
$config->{Monthly} = '';
$config->{Date} = DateTime->now->ymd;

# read config file
Config::Simple->import_from( $config_file, $config);

# merge command line options with config file, command line has precedense
die("Error reading command line options.") unless GetOptions(
	HomeCo::AWS::BackupImageCacher::parameter_match(),
);

# read credentials
( $config->{AWSAccessKey}, $config->{AWSSecret} ) = AWS::Local::Credentials::read_aws_credentials( $config->{AWSCredentials} );

#request parameter check
eval {
	HomeCo::AWS::BackupImageCacher::check_parameters( $config );
	HomeCo::AWS::BackupImageCacher::open_metadata_store( $config );
};
if ( $@ ) {
	die( $@ );
}

if ( $config->{Daily} || $config->{Monthly} ) {
	eval {
		HomeCo::AWS::BackupImageCacher::backup( $config );
	};
	if ( $@ ) {
		die( $@ );
	}
} elsif ( $config->{Cleanup} ) {
	HomeCo::AWS::BackupImageCacher::cleanup( $config );
} else {
	die("Inconsistent parameters. Daily, Monty or Cleanup must be specified.");
}

eval {
	HomeCo::AWS::BackupImageCacher::close_metadata_store( $config );
} if ( $@ ) {
	die( $@ );
}
