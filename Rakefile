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
require "google/apis/drive_v2"
require_relative "extractor"

class String
  def munged
    self.gsub(/(.).*@(.).*\.(.*)/, "\\1*****@\\2*****.\\3")
  end
end

WORKER_COUNT = 16

LOGGER = Logger.new(STDOUT)
LOGGER.level = Logger::INFO
Channel = Concurrent::Channel

desc "write users to csv"
task :write_users_to_csv => [:dotenv] do
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
  worker_pool = Channel.new(capacity: WORKER_COUNT)

  queue = Channel.new(capacity: WORKER_COUNT ** 2)
  extractor = Extractor.new(queue, logger: LOGGER)
  extractor_done = Channel.new(capacity: 1)

  page = Channel.new(capacity: WORKER_COUNT)

  Channel.go {
    extractor.total_pages.times { |n| page << n }
    page << false
  }

  # seed queue with auth0 contacts
  WORKER_COUNT.times do |worker|
    Channel.go {
      LOGGER.debug "auth0 worker #{worker} running"
      page.each do |p|
        begin
          if p == false
            queue << false
            extractor_done << "extractor is done!"
          else
            LOGGER.debug "processing page #{p} via auth0 worker #{worker}"
            extractor.get_users_from_api(page: p)
          end
        rescue StandardError => ex
          LOGGER.warn ex
      end # each
     end # begin
    }
  end

  maropost_client = MaropostApi::Client.new(
    auth_token: ENV["AUTH_TOKEN"],
    account_number: ENV["ACCOUNT_NUMBER"]
  )

  maropost_done = Channel.new(capacity: 1)
  not_found_done = Channel.new(capacity: 1)
  users_in_dnm_done = Channel.new(capacity: 1)

  users_in_dnm = Channel.new(capacity: WORKER_COUNT)
  not_found = Channel.new(capacity: WORKER_COUNT)

  done = Channel.new(capacity: 1)

  WORKER_COUNT.times do |worker|
    Channel.go {
      LOGGER.debug "maropost worker #{worker} running"
      queue.each do |row|
        if row == false
          maropost_done << "reached the end of the queue!"
        else
          begin
            email = row[0]
            auth0_id = row[4]
            LOGGER.debug "#{email.munged} via maropost worker #{worker}"

            contact = maropost_client.contacts.find_by_email(email: email) # triggers the not found exception

            maropost_client.contacts.update(contact_id: contact["id"], params: { contact: { custom_field: { auth0_id: auth0_id } } })
            LOGGER.debug "updated #{email.munged}(#{contact['id']}) with auth0_id #{auth0_id}"

            dnm = maropost_client.global_unsubscribes.find_by_email(email: email)
            if !dnm.has_key?("status")
              users_in_dnm << row.push(contact["id"])
              LOGGER.warn "#{email.munged} in dnm"
            end

          rescue MaropostApi::NotFound => ex
            LOGGER.warn "#{email.munged} not found"
            not_found << row

          rescue StandardError => ex
            LOGGER.warn ex
          end # begin
        end # if
      end # queue
    }
  end

  Channel.go {
    CSV.open("not_found.csv", "wb") do |csv|
      csv << %w(email email_verified first_name last_name auth0_id)
      not_found.each do |row|
        begin
          if row == false
            not_found.close
          else
            csv << row
            LOGGER.debug "#{row[0].munged} added to google sheets not found"
          end
        rescue StandardError => ex
          LOGGER.warn ex
        end # begin
      end
    end
    not_found_done << "not_found goroutine done!"
  }

  Channel.go {
    CSV.open("dnm.csv", "wb") do |csv|
      csv << %w(email email_verified first_name last_name auth0_id maropost_id)
      users_in_dnm.each do |row|
        begin
          if row == false
            users_in_dnm.close
          else
            csv <<  row
            LOGGER.debug "#{row[0].munged} added to google sheets do not mail"
          end
        rescue StandardError => ex
          LOGGER.warn ex
        end # begin
      end
    end
    users_in_dnm_done << "users_in_dnm goroutine done!"
  }

  Channel.go {
    puts ~extractor_done
    puts ~maropost_done
    puts ~users_in_dnm_done
    puts ~not_found_done
    done << "all done!"
  }

  puts ~done

end

desc "Open a pry session preloaded with this library"
task :console do
  pry
end
