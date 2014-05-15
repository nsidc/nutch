require "rspec/core/rake_task"
require "logger"

namespace :nutch do

  DEPLOYMENT_ENVIRONMENTS = {
    :prod => { :solr_host => 'http://54.187.218.185:8983', :solr_path => 'solr'},
    :development => { :solr_host => 'http://localhost:8983', :solr_path => 'solr'}
  }

  @logger = Logger.new(STDOUT)
  @logger.level = Logger::WARN

  # To override the default site config directory ($PWD/site/), call rake with e.g. "site=some_directory"
  NUTCH_SITE_DIR = ENV['NUTCH_HOME'] || "runtime/local"
  NUTCH_CONF_DIR = "#{NUTCH_SITE_DIR}/conf/"
  NUTCH_DATA_DIR = "#{NUTCH_SITE_DIR}/crawldb"
  NUTCH_BIN = "#{NUTCH_SITE_DIR}/bin/nutch"

  NUM_PAGES_TO_FETCH = 100

  SEMAPHORE_FILENAME = "pidfile"

  #TODO [IT, 2012-06-12]: Make the tasks handle Ctrl-C interrupts better - should bail on the whole task instead of needing multiple kills to actually stop doing anything

  desc "Runs a new crawl/index cycle if one isn't already running"
  task :iter, :environment do |t, args|
    set_environment args[:environment]

    if get_semaphore?
      begin
        run_iter
      rescue Exception => ex
        @logger.fatal ex.message
      ensure
        delete_semaphore
      end
    else
      @logger.warn "Nutch is already nutching...  Please try later."
    end
  end


  desc "Prepares the Nutch install for fresh crawl starting with the URLs in the seed file"
  task :clear_and_reseed_nutch_crawl_db do
    fix_nutch_bin_attrs

    run "rm -rf #{NUTCH_DATA_DIR}"
    run_nutch "inject #{NUTCH_DATA_DIR} #{NUTCH_SITE_DIR}/bin/urls/seeds.txt"
  end


  desc "Crawl the next set of pages in the crawl list"
  task :crawl do
    run_nutch "generate #{NUTCH_DATA_DIR} #{NUTCH_DATA_DIR}/segments -topN #{NUM_PAGES_TO_FETCH} "

    s1 = get_most_recent_segment_dir_name()
    run_nutch "fetch #{s1}"
    run_nutch "parse #{s1}"
    run_nutch "updatedb #{NUTCH_DATA_DIR} #{s1}"

    # Invert links so Solr can ingest the data
    run_nutch "invertlinks #{NUTCH_DATA_DIR}/linkdb -dir #{NUTCH_DATA_DIR}/segments"
  end


  desc "Index the last crawl's results into Solr. Supported environments are integration, qa, staging, production and development"
  task :index, :environment do |t, args|
    set_environment args[:environment]

    run_nutch "solrindex #{solr_url} #{NUTCH_DATA_DIR}/ -linkdb #{NUTCH_DATA_DIR}/linkdb/ #{NUTCH_DATA_DIR}/segments/*"

    # Remove segments directories
    #TODO: Only do this if the indexing was successful
    FileUtils.rm_rf "#{NUTCH_DATA_DIR}/segments"

    # Remove pages that aren't there any more from Solr's index
    # run_nutch "solrclean #{NUTCH_DATA_DIR}/ #{solr_url}"
  end


  desc "Clear everything (yes, everything) out from the Solr instance"
  task :clear_solr, :environment do |t, args|
    set_environment args[:environment]
    run "curl -kv '#{solr_url}/update?stream.body=<delete><query>*:*</query></delete>&commit=true'"
  end

  def set_environment(env)
    env = env || @environment  # Use a previously set environment if one isn't passed, and one has already been set.
    fail "No environment set" unless env
    environment = env.to_sym
    fail("No such environment (#{env})") unless DEPLOYMENT_ENVIRONMENTS.has_key? environment
    @environment = environment
  end

  def environment_settings
    return DEPLOYMENT_ENVIRONMENTS[@environment]
  end

  def solr_url
    solr_host = environment_settings[:solr_host]
    solr_path = environment_settings[:solr_path]
    return "#{solr_host}/#{solr_path}"
  end

  def run(command)
    @logger.info command
    system "#{command} >&2"
  end

  def run_nutch(command)
    run "NUTCH_CONF_DIR=#{NUTCH_CONF_DIR} #{NUTCH_BIN} #{command}"
  end

  def get_most_recent_segment_dir_name
    `ls -d #{NUTCH_DATA_DIR}/segments/2* | tail -1`
  end

  def fix_nutch_bin_attrs
    run "chmod a+x #{NUTCH_BIN}"
  end

  # Tries to exclusively open a sempahore file.
  # If the file already exists, return false.
  def get_semaphore?
    write_pid_to_pidfile
  end

  # Removes the semaphore file
  def delete_semaphore
    @logger.info "Removing semaphore..."
    File.delete(SEMAPHORE_FILENAME)
  end

  def write_pid_to_pidfile
    begin
      # The EXCL option ensures Ruby will not create a file if it already exists
      File.open(SEMAPHORE_FILENAME, File::CREAT|File::RDWR|File::TRUNC|File::EXCL) do |pidfile|
        pid = Process.pid
        @logger.info "Creating semaphore for process #{pid}..."
        pidfile.puts pid
        return true
      end
    rescue Errno::EEXIST
      return false
    rescue Exception => ex
      # If there's another exception, do our best to clean up.
      delete_semaphore
      @logger.error ex
      raise ex
    end
  end

  def run_iter
    Rake::Task["nutch:crawl"].execute
    Rake::Task["nutch:index"].execute
  end

end

