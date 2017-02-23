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
require "g_sheets"
require_relative "extractor"


desc "write users to csv"
task :write_users_to_csv => [:dotenv] do
  LOGGER = Logger.new(STDOUT)
  LOGGER.level = Logger::DEBUG

  Channel = Concurrent::Channel

  queue = Channel.new
  extractor = Extractor.new(queue, logger: LOGGER)

  extractor.get_users_from_api

  done = Channel.new(capacity: 1)
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

  CLIENT_ID = ENV["GOOGLE_CLIENT_ID"]; CLIENT_SECRET = ENV["GOOGLE_CLIENT_SECRET"]
  authenticator = GSheets::Oauth::Offline.new(CLIENT_ID, CLIENT_SECRET)

  REFRESH_TOKEN = ENV["REFRESH_TOKEN"]
  access_token = authenticator.get_access_token(refresh_token: REFRESH_TOKEN)
  session = GSheets::Session.new(access_token: access_token)

  SHEET_ID = ENV["SHEET_ID"]
  ss = GSheets::SpreadSheet.new(session: session, id: SHEET_ID)
  not_found_sheet = ss.sheets[0]
  users_in_dnm_sheet = ss.sheets[1]

  Channel = Concurrent::Channel
  queue = Channel.new
  worker_pool = Channel.new(capacity: 16)

  extractor = Extractor.new(queue, logger: LOGGER)
  extractor.get_users_from_api

  users_in_dnm = Channel.new(capacity: 16)
  not_found = Channel.new(capacity: 16)

  users_in_dnm_done = Channel.new(capacity: 1)
  not_found_done = Channel.new(capacity: 1)
  workers_done = Channel.new(capacity: 1)

  LOGGER.debug "starting the updater channel"
  maropost_client = MaropostApi::Client.new(
    auth_token: ENV["AUTH_TOKEN"],
    account_number: ENV["ACCOUNT_NUMBER"]
  )

  Channel.go {
    extractor.total_records.times do |counter|
      row = ~queue
      email = row.first
      Channel.go {
        begin
          maropost_client.contacts.find_by_email(email: email) # trigger not found
          if maropost_client.global_unsubscribes.find_by_email(email: email)
            users_in_dnm << row
            LOGGER.warn "#{email} in dnm"
          end
        rescue MaropostApi::NotFound => ex
          LOGGER.warn "#{email} not found"
          not_found << row
        rescue StandardError => ex
          LOGGER.warn ex
        end
        worker_pool << "#{counter + 1} of #{extractor.total_records} #{email} processed"
      }
    end
  }

  Channel.go {
    not_found.each do |row| LOGGER.debug not_found_sheet.append(row); end
    not_found_done << "not_found done!"
  }

  Channel.go {
    users_in_dnm.each do |row| LOGGER.debug users_in_dnm_sheet.append(row); end
    users_in_dnm_done << "users_in_dnm done!"
  }

  Channel.go {
    counter = 0
    worker_pool.each do |pool|
      counter += 1
      LOGGER.debug "#{pool} => #{counter} / extractor.total_records"
      if counter >= extractor.total_records
        worker_pool.close
        not_found.close
        users_in_dnm.close
        workers_done << "wokers done!"
      end
    end
  }

  LOGGER.debug ~workers_done
  LOGGER.debug ~not_found_done
  LOGGER.debug ~users_in_dnm_done

end

desc "Open a pry session preloaded with this library"
task :console do
  pry
end