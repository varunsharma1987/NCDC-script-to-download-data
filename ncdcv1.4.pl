package HAS;

use strict;
use LWP;
use Data::Dumper;
use File::Basename;
use File::Path;
use File::Temp;
use Net::FTP;
use Time::Local;

use IO::CaptureOutput;

use FindBin;
use lib "$FindBin::Bin";
#use NexradUtil;

use vars qw($MAX_DOWNLOAD_ERRORS $MAX_EXTRACT_ERRORS);

# Tolerate this # of individual file errors 
# before declaring the whole operation an error
$MAX_DOWNLOAD_ERRORS = 1;
$MAX_EXTRACT_ERRORS = 10;

my $usage = <<USAGE;
Usage: ncdc.pl command <command arguments>

List of available commands:

	Command 	Arguments
	order		email station start_date end_date [workdir] [start_time] [end_time]
	check		orderid [workdir]
	download	orderid [workdir]
	extract		start_time end_time outdir
USAGE

if($#ARGV <= 0)
{
	print $usage;
}
else
{
	my $command = shift(@ARGV);
	if($command eq 'order')
	{
		&order(@ARGV);
	}
	elsif($command eq 'check')
	{
		&check_pending(@ARGV);
	}
	elsif($command eq 'download')
	{
		&download(@ARGV);
	}
	elsif($command eq 'extract')
	{
		&extract(@ARGV);
	}
	else
	{
		print $usage;
	}
}

sub msg
{
	print @_;
}

############################################################
# is_available()
#
#   Check if HAS system seems to be available. Returns true or false.
############################################################
sub is_available
{
    my $browser = LWP::UserAgent->new();

    my $url = "http://has.ncdc.noaa.gov/pls/plhas/has.dsselect";
    my $response = $browser->get($url);
    
    if (! ($response->is_success() && $response->content_type =~ m|text/html|))
    {
	msg("Could not connect to HAS or got unexpected content type\n");
	return 0;
    }
    
    if ($response->content() =~ m|The HDSS Access System is currently unavailable|s)
    {
	msg("HDSS Access System is currently unavailable\n");
	return 0;
    }
    
    return 1;
}

############################################################
# order()
#
#   Place a new order on the HAS system. This requires navigating two
#   webpages at the NCDC/HAS site. The first page selects the radar
#   station and dates and the second page selects individual tar files
#   for download.
#
# Input:
#   $station, 
#   $start_date, $enddate  - (YYYY-MM-DD, inclusive)
#   $start_time, $endtime  - start/end times, interpreted as follows
#       numeric - hours UTC
#       hash ref. - time specified relative to sunrise, sunset, e.g
#
#        { base => 'sunrise',     # 'sunrise' | 'sunset'
#	   offset => -30  }       # time offset in minutes
#   $workdir - work directory. Used only to save HTML response upon error
#
# Returns
#   $orderid          - HAS order id
#   \@selected_files  - List of files ordered, with entries like this:
#
#          {
#            'path' => '/aab/6500/6500_03/KDOX/201011/KDOX2010110711.tar',
#            'id' => '43792177',
#            'size' => '1955840'
#          }
#
############################################################


sub order {
    
    my ($email, $station, $start_date, $end_date, $workdir, $start_time, $end_time) = @_;
	
    #msg("HAS::order, station=$station, dates: $start_date -- $end_date");
    #msg("Start time: " . Dumper($start_time));
    #msg("End time: " . Dumper($end_time));

    #$workdir= "/home3/datafac1/work/ncdc";

    $workdir = $workdir || ".";

    my $browser = LWP::UserAgent->new();
    my $url = "http://has.ncdc.noaa.gov/pls/plhas/HAS.FileSelect";

    my ($start_year, $start_month, $start_day) = split(/-/, $start_date);
    my ($end_year, $end_month, $end_day) = split(/-/, $end_date);

    #print "$start_year, $start_month, $start_day\n";
    #print "$end_year, $end_month, $end_day\n";

    # ensure proper formatting
    #for my $val (\$start_year, \$end_year)
    #{
#	$$val = sprintf("%04d", $$val);
#    }
    
#    for my $val (\$start_month, \$start_day, \$end_month, \$end_day)
#    {
#	$$val = sprintf("%02d", $$val);
#    }

    ############################################################
    # Get list of files
    ############################################################
    #msg("Getting list of available files");

    my ($response, $orderid, @selected_files);
    eval {

	$response = $browser->post( $url,
				    [
				    'satdisptype' => 'N/A',
				    'stations' => $station,
				    'station_lst' => '',
				    'typeofdata' => 'RADAR',
				    'dtypelist' => '',
				    'begdatestring' => '',
				    'enddatestring' => '',
				    'begyear' => $start_year,
				    'begmonth' => $start_month,
				    'begday' => $start_day,
				    'beghour' => '',
				    'begmin' => '',
				    'endyear' => $end_year,
				    'endmonth' => $end_month,
				    'endday' => $end_day,
				    'endhour' => '',
				    'endmin' => '',
				    'timeselhourlist' => '',
				    'timeselectminlist' => '',
				    'timeselectbuffer' => '',
				    'filesizefilteroperator' => '',
				    'filesizefiltervalue' => '',
				    'gvartype' => '',
				    'receiving_stations' => '',
				    'outmed' => 'FTP3',
				    'outpath' => '',
				    'pri' => '500',
				    'altdsname' => '',
				    'altstations1' => '',
				    'altstations2' => '',
				    'datasetname' => '6500',
				    'directsub' => 'N',
				    'emailadd' => $email,
				    'outdest' => 'FILE',
				    'applname' => '',
				    'subqueryby' => 'STATION',
				    'tmeth' => 'Awaiting-Data-Transfer'
				    ]
	    );	

	die "Can't get $url -- ". $response->status_line() if not $response->is_success();
	die "Response not HTML ( ", $response->content_type, " )" unless $response->content_type eq 'text/html';
	
	my $content = $response->content();

	my ($filelist) = ($content =~ m|(<select.*</SELECT>)|gsi);

	my $annotatedEmail = "";
	if ($content =~ m|<INPUT TYPE="hidden" NAME="emailadd" VALUE="(.*?)">|)
	{
	    $annotatedEmail = $1;
	}
	else
	{
	    die "Cannot find email address";
	}

	my @triples = ($filelist =~ m|<OPTION SELECTED VALUE="(\d+)"> (.*?.tar)\s+-\s+(\d+)\s|gs);
	
	my @files = ();
	while (@triples)
	{
	    my ($id, $path, $size);
	    ($id, $path, $size, @triples) = @triples;
	    
	    push @files, {id => $id,
			  path => $path,
			  size => $size};
	}
	
	############################################################
	# Request selected files
	############################################################

	@selected_files = ();

	for my $file (@files)
	{
	    my ($filename, $path) = basename($$file{path});

	    my ($station, $yyyy, $mm, $dd, $file_start_hour, $file_end_hour) = parse_tar_filename($filename);
	    
	    my $file_start_time = sprintf("%02d0000", $file_start_hour);
	    my $file_end_time   = sprintf("%02d5959", $file_end_hour);

	    #my $start_time = parse_time($station, $yyyy, $mm, $dd, $start_time);
	    #my $end_time = parse_time($station, $yyyy, $mm, $dd, $end_time);

	    $start_time= $start_time || "000000";
	    $end_time= $end_time || "235959";

	    if ($start_time le $end_time)
	    {
			# If start time precedes end time, get all data in the interval [start, end]
			if ($file_start_time le $end_time && $file_end_time ge $start_time)
			{
				push @selected_files, $file;
			}
			else
			{
				print "Azsaza1\n";
			}
	    }
	    else
	    {
			# Otherwise, interpret the request as going from start
			# time today to end time tomorrow (spanning midnight)
			# Get all data either in intervals [start, midnight]
			# or [midnight, end]

			if ( $file_end_time ge $start_time || $file_start_time <= $end_time)
			{
				push @selected_files, $file;
			}
			else
			{
				print "Azsaza2\n";
			}
	    }
	}

	die "No files selected" unless (@selected_files);

	#msg(sprintf("Ordering %d/%d files", scalar(@selected_files), scalar(@files)));

	$url = "http://has.ncdc.noaa.gov/pls/plhas/HAS.TarFileSelect";
	
	my $options = [ 
	    stations => $station,
	    typeofdata => "RADAR", 
	    FILESEL    => "SELECTED", 
	    timeselhourlist => "",
	    timeselectmins  => "",
	    receiving_stations => "",
	    timeselectbuffer => "",
	    filesizefilteroperator => "",
	    filesizefiltervalue => "",
	    gvartype => "",
	    begyear => $start_year,
	    begmonth => $start_month,
	    begday => $start_day,
	    beghour => "",
	    begmin => "",
	    endyear => $end_year,
	    endmonth => $end_month,
	    endday => $end_day,
	    endhour => "",
	    endmin => "",
	    outmed => "FTP",
	    datasetname => 6500,
	    emailadd => $annotatedEmail,
	    outdest => "FILE",
	    applname => "",
	    subqueryby => "STATION",
	    altdsname => "",
	    numstats => 1,
	    altstation => "",
	    untar_sel => "N",
	    untar_sel => "N",
	    tmeth => "Awaiting-Data-Transfer",
	    pri => "500",
	    directsub => "N",
	    outpath => "",
	    ];

	for my $file (@selected_files)
	{
	    $options = [@$options, selectedfiles => $$file{id}];
	}

	$response = $browser->post( $url, $options );
	die "Can't get $url -- ", $response->status_line() unless $response->is_success();
	die "Response not HTML ( ", $response->content_type, " )" unless $response->content_type eq 'text/html';
	
	$content = $response->content();

	#if ($content =~  m|Your HAS data request: <B><FONT COLOR="CC3300">HAS(\d+)</FONT>|)
	if ($content =~  /<tr><td class="var">Order Number:<\/td><td class="val">HAS(\d+)/)
	{
	    $orderid = $1;
	}
	else
	{
	    die "No order ID";
	}

	print("Success. HAS order # is $orderid\n");
    };
    if ($@)
    {
	my $errfile = "$workdir/err.html";
	if ($response && $response->content())
	{
	    warn "ERROR: saving HTML response to $errfile";
	    save_file($errfile, $response->content());	
	}
	die $@;
    }

    my @filenames = map {basename($$_{path})} @selected_files;

    return ($orderid, \@filenames);
}


############################################################
# check_pending()
#
# Checks the status of an HAS order to see if it is ready 
# for download.
#
# Inputs:
#   $orderid - HAS order #
#   $workdir - Work directory. Used only to save .html repsonse upon
#              error
#
# Returns:
#   $nfiles - total # of files in the order
#   $percent_complete - ready for download if $percent_complete == 100
############################################################
sub check_pending {

    my ($orderid, $workdir) = @_;
    
    $workdir = $workdir || ".";

    my ($response, $nfiles, $percent_complete);
    eval {
	my $browser = LWP::UserAgent->new();
	my $url = "http://cdo.ncdc.noaa.gov/cgi-bin/HAS/HAS_reqlookup.pl?&requestid=$orderid";
	$response = $browser->get($url);

	die "Can't get $url -- ", $response->status_line() unless $response->is_success();
	die "Response not HTML ( ", $response->content_type, " )" unless $response->content_type eq 'text/html';
	
	my $content = $response->content();
	($nfiles, $percent_complete) = ($content =~ m|Your request of (\d+) files\s+is\s+<font.*?>([0-9\.]+)</font>% complete|s);
	die "Failed parse" if not defined $percent_complete;
    };
    if ($@)
    {
	my $errfile = "$workdir/err.html";
	if ($response && $response->content())
	{
	    warn "ERROR: saving HTML response to $errfile";
	    save_file($errfile, $response->content());	
	}
	die $@;
    }

	print "Total # of files in the order: $nfiles\nPercentage complete: $percent_complete\n";
    return ($nfiles, $percent_complete);
}

############################################################
# download()
#
# Downloads all files from an HAS order.
#
# Inputs:
#  $orderid - the HAS order #
#  $workdir - where to save the downloaded tar files
#
# Returns
#  \@downloaded_files - files that were successfully downloaded (local filenames)
#  \@error_files -      files that could not be downloaded
#
# This routine tolerates $MAX_DOWNLOAD_ERRORS individual download
# errors on before raising an error.
############################################################
sub download {
   
    my ($orderid, $workdir) = @_;

    $workdir = $workdir || '.';

    my $ftp = ftp_connect();

    $ftp->cwd("/pub/has/HAS$orderid")
	or die "Cannot change to directory /pub/has/HAS$orderid ", $ftp->message();
    
    my @files = $ftp->ls();

    my $errors = 0;
    my @downloaded_files = ();
    my @error_files = ();
    for my $file (sort @files) {
	
	my $dst = "$workdir/$file";
	my $filesize = $ftp->size($file);

	if (-f $dst && $filesize == -s $dst)
	{
	    msg("Already have file $file (correct size)\n");
	    push @downloaded_files, $dst;
	}
	else
	{
	    msg("Downloading file $file ($filesize bytes)\n");
	    if ($ftp->get($file, $dst))
	    {
		msg("Succesfully downloaded $file\n");
		system("sync");
		push @downloaded_files, $dst;
	    }
	    else
	    {
		msg("Download failed: " . $ftp->message() . "\n");
		$errors++;
		push @error_files, $dst;
		msg("Error # $errors (max: $MAX_DOWNLOAD_ERRORS)\n");
		if ($errors > $MAX_DOWNLOAD_ERRORS)
		{
		    msg("Exceeded maximum # of errors\n");
		    die "Exceeded maximum # of errors";
		}
	    }
	}
    }
    $ftp->quit() if defined $ftp;

    return \@downloaded_files, \@error_files;
}


############################################################
# A do-nothing function for use as a default function reference
############################################################
sub noop {}

############################################################
# extract()
#
# Extract tar files from current directory to obtain data files for individual radar scans.
#
# Inputs:
#   $start_time       - Specify which files to extract. See
#   $end_time           comments in HAS::order() for the format 
#                       of these variables.
#
#   $RADARHOME        - Root of directory structure where files
#                       will be extracted
#
#   $user_success_fun - Optional user-defined function reference
#                       which will be called  successful extraction
#                       with the filename of the succesfully extracted
#                       .tar file as the single argument.
#                       
#                       E.g., use \&unlink to remove the file after
#                       successful extraction
#
#   $user_error_fun   - Same as above, but caled after failed
#                       extraction.
#
# Returns: none
#
# Side-effects:
#
#   Populates the $RADARHOME directory tree with successfully
#   extracted files, using the following naming convention
#  
#   $RADARHOME/<station>/<yyyy>/<mm>/<dd>/<scan_filename>
#
# This routine tolerates $MAX_EXTRACT_ERRORS individual extraction
# errors raising an error.
############################################################
sub extract {
    
    my ($start_time, $end_time, $RADARHOME
#	, $user_success_fun, $user_error_fun
	) = @_;
        
    my $errors = 0;

#    if (not defined $user_success_fun) { $user_success_fun = \&noop; }
#    if (not defined $user_error_fun)   { $user_error_fun = \&noop; }

	my @files;
	
	opendir(DIR, '.') or die $!;
    while (my $file = readdir(DIR)) {
        push @files, $file if ($file =~ m/\.tar$/);
    }
    closedir(DIR);
	
    my @extracted_files = ();

    for my $file (@files)
    {
	my $base = basename($file);
	msg("Extracting $base\n");
	next;

	eval {

	    my ($station, $year, $month, $day) = parse_tar_filename($base);	
	    my $outdir = "$RADARHOME/stations/$station/$year/$month/$day";
	    if (! -d $outdir)
	    {
		mkpath($outdir) or die "Can't make directory $outdir";
	    }

	    #my $start_time = parse_time($station, $year, $month, $day, $start_time);
	    #my $end_time = parse_time($station, $year, $month, $day, $end_time);

	    # First get a list of the files in the tarball
	    my ($stdout, $stderr, $success, $exit_code) =
		IO::CaptureOutput::capture_exec("tar -tf $file");
	    
	    if (!$success && $stderr =~ m/Archive contains obsolescent base-64 headers/) 
	    {
		warn $stderr;
		warn "Attempting to recover from known tar error (obsolescent headers)";
	    }
	    elsif (! $success)
	    {
		warn $stderr;
		die "tar command failed on $file: exit code $exit_code";
	    }

	    my @contents = split(/\s+/, $stdout);
	    @contents = sort @contents;

	    my @selected_files = ();
	    for my $scanfile (@contents)
	    {
		my ($scan_time, $version, $compression);

		if ($scanfile =~ /\w{4}\d{8}_(\d{6})(_V\d+)?\.(Z|gz)/)
		{
		    $scan_time = $1;
		    $version = $2;
		    $compression = $3;
		}
		else
		{
		    die "Unrecognized naming convention for file $scanfile";
		}
		
		if ( $start_time le $end_time)
		{
		    # start < end: get anything in consecutive time interval [start, end]
		    if ($scan_time ge $start_time && $scan_time le $end_time)
		    {
			push @selected_files, $scanfile;
		    }
		}
		else
		{
		    # start > end: overnight time interval 
		    #    Get anything from [midnight, end] or [start, midnight]
		    if ($scan_time le $end_time || $scan_time ge $start_time)
		    {
			push @selected_files, $scanfile;
		    }
		}
	    }

	    msg(sprintf("%d files selected", scalar(@selected_files)));
	    
	    if (@selected_files)
	    {

		my $cmd = join(" ", "tar -C $outdir -xf $file", @selected_files);
		my ($stdout, $stderr, $success, $exit_code) =
		    IO::CaptureOutput::capture_exec($cmd);
		
		if (!$success)
		{
		    if ($stderr =~ m/Archive contains obsolescent base-64 headers/) {
			msg($stderr);
			msg("Attempting to recover from known tar error (obsolescent headers)\n");
			push @extracted_files, map {"$outdir/$_" } @selected_files;
		    }
		    else
		    {
			msg($stderr);
			die "tar command failed on $file: exit code $exit_code";
		    }
		}
		else
		{
		    msg("Success.\n");
		    push @extracted_files, map {"$outdir/$_" } @selected_files;
		}		    
	    }
	};
	if ($@)
	{
	    chomp($@);
	    $errors++;
#	    &$user_error_fun($file);
	    msg("Error: $@\n");
	    msg("Error $errors/$MAX_EXTRACT_ERRORS\n");
	    if ($errors > $MAX_EXTRACT_ERRORS)
	    {
		msg("Exceeded maximum # of errors\n");
		die "Exceeded maximum # of errors";
	    }
	}
	else
	{
#	    &$user_success_fun($file);
	}
    }
    
    return \@extracted_files;
}

############################################################
# connect to NCDC ftp site
############################################################
sub ftp_connect
{
    my ($email) = @_;

    my $ftp = Net::FTP->new("ftp3.ncdc.noaa.gov", Debug => 0) 
	or die "Cannot connect to ftp3.ncdc.noaa.gov: $@";
    
    $ftp->login("anonymous", $email)
	or die "Cannot login ", $ftp->message();
    
    $ftp->binary() or die "Can't change to binary mode";
    return $ftp;
}

############################################################
# save_file: prints text to file
############################################################
sub save_file {
    my ($file, $contents) = @_;
    open (OUT, ">$file") or die "Can't open file"; 
    print OUT $contents;
    close OUT;
}


############################################################
# parse .tar filename
############################################################
sub parse_tar_filename {

    my ($filename) = @_;

    my ($station, $yyyy, $mm, $dd, $file_start_hour, $file_end_hour);

    if ($filename =~ /^([[:alpha:]]{4})(\d{4})(\d{2})(\d{2})(\d{2})\.tar$/)
    {
	# One-hourly tar files from sometime in 2008 onward
	$station = $1;
	$yyyy = $2;
	$mm = $3;
	$dd = $4;
	$file_start_hour = $5;
	$file_end_hour = $file_start_hour;
    }
    elsif ($filename =~ /^([[:alpha:]]{4})(\d{4})(\d{2})(\d{2})_(\d{2})-(\d{2})\.tar$/)
    {
	# 8-hourly tarfiles from 2007 and before
	$station = $1;
	$yyyy = $2;
	$mm = $3;
	$dd = $4;
	$file_start_hour = $5;
	$file_end_hour = $6;
    }
    #elsif ($filename =~ /^NWS_NEXRAD_NXL2(LG|SR|DP)_([[:alpha:]]{4})_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\.tar$/)
    elsif ($filename =~ /^NWS_NEXRAD_NXL2(LG|SR|DP)_(.{4})_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\.tar$/)
    {
	# LG = legacy resolution
	# SR = super resolution
	# DP = dual polarization

	my $type_code    = $1;
	$station         = $2;
	my $start_year   = $3;
	my $start_month  = $4;
	my $start_day    = $5;
	my $start_hour   = $6;
	my $start_minute = $7;
	my $start_second = $8;
	my $end_year     = $9;
	my $end_month    = $10;
	my $end_day      = $11;
	my $end_hour     = $12;
	my $end_minute   = $13;
	my $end_second   = $14;
	

	die "File spans multiple years"  if ($start_year != $end_year);
	die "File spans multiple months" if ($start_month != $end_month);
	die "File spans multiple days"   if ($start_day != $end_day);

	if (! ($start_minute == 0 && $start_second == 0 && $end_minute == 59 && $end_second == 59) )
	{
	    die "File does not cover complete hour";
	}

	$yyyy = $start_year;
	$mm = $start_month;
	$dd = $start_day;
	$file_start_hour = $start_hour;
	$file_end_hour = $end_hour;
    }
    else
    {
	die "Unrecognized naming convention for tar file ($filename)";
    }

    return ($station, $yyyy, $mm, $dd, $file_start_hour, $file_end_hour);
}
