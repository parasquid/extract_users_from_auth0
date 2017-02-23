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

  Channel.go {
    begin
      LOGGER.debug "starting writer channel"
      extractor.write_user_row_to_csv
    rescue StandardError => ex
      LOGGER.warn ex
    end
  }

  LOGGER.debug ~done
end
