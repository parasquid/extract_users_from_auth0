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
        while counter < extractor.total_records do
          row = ~queue
          csv <<  row
          LOGGER.debug "#{counter} of #{extractor.total_records} processed"
          counter += 1
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

  Channel.go {
    begin
      LOGGER.debug "starting writer channel"
      extractor.update_maropost_with_auth0_id
    rescue StandardError => ex
      LOGGER.warn ex
    end
  }

  LOGGER.debug ~done
end
