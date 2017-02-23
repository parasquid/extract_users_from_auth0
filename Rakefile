require "dotenv/load"
require "dotenv/tasks"
require "pry"
require "auth0"
require 'uri'
require 'net/http'
require "concurrent"
require "concurrent-edge"
require "csv"
require "json"
require "jwt"
require "maropost_api"
require_relative "extractor"


desc "write users to csv"
task :write_users_to_csv => [:dotenv] do
  LOGGER = Logger.new(STDOUT)
  LOGGER.level = Logger::DEBUG

  Channel = Concurrent::Channel

  queue = Channel.new
  done = Channel.new(capacity: 1)
  extractor = Extractor.new(queue, done, logger: LOGGER)

  extractor.get_users_from_api

  LOGGER.debug "starting writer channel"
  Channel.go {
    begin
      LOGGER.debug "opening the csv for writing"
      counter = 0
      CSV.open("accounts.csv", "wb") do |csv|
        csv << ["email", "email_verified", "given_name", "family_name"]
        LOGGER.debug "reading the queue #{extractor.total_records} total records"
        extractor.total_records.times do |counter|
          row = ~queue
          csv <<  row
          LOGGER.debug "#{counter} of #{extractor.total_records} processed"
        end
      end
      done << "all done!"
    rescue StandardError => ex
      LOGGER.warn ex
    end
  }

  LOGGER.debug ~done
end

desc "update maropost with auth0_id"
task :update_maropost_with_auth0_id => [:dotenv] do
  LOGGER = Logger.new(STDOUT)
  LOGGER.level = Logger::DEBUG

  Channel = Concurrent::Channel

  queue = Channel.new
  done = Channel.new(capacity: 1)
  extractor = Extractor.new(queue, done, logger: LOGGER)

  extractor.get_users_from_api

  worker_pool = Concurrent::Channel.new(capacity: 16)

  LOGGER.debug "starting the updater channel"
  extractor.total_records.times do |counter|
    row = ~queue
    email = row.first
    client = MaropostApi::Client.new(
      auth_token: ENV["AUTH_TOKEN"],
      account_number: ENV["ACCOUNT_NUMBER"]
    )

    Channel.go {
      begin
        client.contacts.find_by_email(email: email)
      rescue MaropostApi::NotFound => ex
        LOGGER.warn email
      end
      worker_pool << "#{counter + 1} of #{extractor.total_records} #{email} processed"
    }

  end

  # drain all workers before quitting
  extractor.total_records.times do
    LOGGER.debug ~worker_pool # blocks when there's no worker in the queue
  end
end

desc "Open a pry session preloaded with this library"
task :console do
  pry
end