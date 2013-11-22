use strict;
use Config::Simple;
use Getopt::Long;

#calculate config file name
my $config = {}; #must be empty hash for Config::Simple
my $config_file = $0; $config_file =~ s/\.([^\.]+)$/\.cfg/;
die("No config file $config_file!") unless -f $config_file;

#read config file
Config::Simple->import_from( $config_file, $config);
die("No BaseThumbs defined") unless defined $config->{BaseThumbs};
die("BaseThumbs does not exist") unless -d $config->{BaseThumbs};
die("No BaseImageCache defined") unless dfined  $config->{BaseImageCache};
die("BaseImageCache does not exist") unless -d $config->{BaseImageCache};
die("VaultRegion does not exist. Set an AWS Region") unless -d $config->{VaultRegion};
die("VaultName does not exist. Set an existing AWS Glacier Vault") unless -d $config->{VaultRegion};

my $archive_filename_map = 'archive_filename'; #default archive to filename map file
my $credential_file_path = '.aws_credentials.txt'; #default credential file
my $region = $config->{VaultRegion}; #default region
my $vault = $config->{VaultName}; #no default vault
my $result = GetOptions(
	"archivefilenamemap=s" => \$archive_filename_map,
	"vault=s" => \$vault,
	"region=s" => \$region,
	"credentials=s" => \$credential_file_path,
);


