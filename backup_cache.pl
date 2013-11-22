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

# merge commandl line options with config file
die("Error reading command line options.") unless GetOptions(
	"vault=s" => $config->{VaultName},
	"region=s" => $config->{VaultRegion},
	"credentials=s" => $config->{AWSCredentials},
	"daily" => $config->{Daily},
	"monthly" => $config->{Monthly},
	"date=s" => $config->{Date},
);

# read credentials
( $config->{AWSAccessKey}, $config->{AWSSecret} ) = AWS::Local::Credentials::read_aws_credentials( $config->{AWSCredentials} );

#request parameter check
eval {
	HomeCo::AWS::BackupImageCacher::check_parameters( $config );
};
if ( $@ ) {
	die( $@ );
}

eval {
	HomeCo::AWS::BackupImageCacher::backup( $config );
};
if ( $@ ) {
	die( $@ );
}






