package Local::AWS::Credentials;

# ============================================================================
# Handles plain files with AWS credential

# ============================================================================
use strict;
use warnings;
use Carp qw( croak );

# ============================================================================
require Exporter;
use vars qw($VERSION @ISA @EXPORT);

@ISA = qw(Exporter);
@EXPORT = qw(read_aws_credentials);

# ============================================================================
# Reads credentials from plain file
# access_key\n
# secret_key\n
sub read_aws_credentials {
  my ( $stash_file ) = shift || '.aws_credentials.txt';

	open ( AWS_STASH, $stash_file ) || croak('Could not open AWS credential file');

	my $aws_access_key = <AWS_STASH>;
	my $aws_secret_key = <AWS_STASH>;

	chomp ( $aws_access_key );
	chomp ( $aws_secret_key );

	close ( AWS_STASH );

	return ( $aws_access_key, $aws_secret_key );
}
# ============================================================================

1;
