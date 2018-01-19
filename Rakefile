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
require_relative "simple_extractor"

class String
  def munged
    self.gsub(/(.).*@(.).*\.(.*)/, "\\1*****@\\2*****.\\3")
  end
end

WORKER_COUNT = 16

LOGGER = Logger.new(STDOUT)
LOGGER.level = Logger::DEBUG
Channel = Concurrent::Channel

desc "write users to csv (simple)"
task :write_users_to_csv_simple => [:dotenv] do
  extractor = SimpleExtractor.new(logger: LOGGER)
  LOGGER.debug "total records: #{extractor.total_records} in #{extractor.total_pages} of #{extractor.per_page} pages"

  LOGGER.debug "opening the csv for writing"
  CSV.open("accounts.csv", "wb") do |csv|
    csv << ["email", "email_verified", "given_name", "family_name"]

    extractor.total_pages.times do |page|
      rows = extractor.get_users_from_api(page: page)
      rows.each { |row| csv << row }
      LOGGER.debug "#{page} of #{extractor.total_pages} processed"
    end
  end
  LOGGER.debug "all done!"

end

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
            LOGGER.info "processing page #{p} via auth0 worker #{worker}"
            extractor.get_users_from_api(page: p)
          end
        rescue StandardError => ex
          LOGGER.warn ex
      end # each
     end # begin
    }
  end

  mvhq_maropost_client = MaropostApi::Client.new(
    auth_token: ENV["MVHQ_AUTH_TOKEN"],
    account_number: ENV["MVHQ_ACCOUNT_NUMBER"]
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
          not_found << false
          users_in_dnm << false
          queue.close
        else
          begin
            email = row[0]
            auth0_id = row[4]
            LOGGER.debug "#{email} via maropost worker #{worker}"

            contact = mvhq_maropost_client.contacts.find_by_email(email: email) # triggers the not found exception

            # mvhq_maropost_client.contacts.update(contact_id: contact["id"], params: { contact: { custom_field: { auth0_id: auth0_id } } })
            LOGGER.info "updated #{email}(#{contact['id']}) with auth0_id #{auth0_id}"

            dnm = mvhq_maropost_client.global_unsubscribes.find_by_email(email: email)
            if !dnm.has_key?("status")
              users_in_dnm << row.push(contact["id"])
              LOGGER.info "#{email} in dnm"
            end

          rescue MaropostApi::NotFound => ex
            LOGGER.info "#{email} not found"
            not_found << row

          rescue StandardError => ex
            LOGGER.warn ex
          end # begin
        end # if
      end # queue
    }
  end

  mvt_maropost_client = MaropostApi::Client.new(
    auth_token: ENV["MVT_AUTH_TOKEN"],
    account_number: ENV["MVT_ACCOUNT_NUMBER"]
  )

  mvr_maropost_client = MaropostApi::Client.new(
    auth_token: ENV["MVR_AUTH_TOKEN"],
    account_number: ENV["MVR_ACCOUNT_NUMBER"]
  )

  Channel.go {
    CSV.open("not_found.csv", "wb") do |csv|
      csv << %w(email email_verified first_name last_name auth0_id mvt_contact_id mvr_contact_id)
      not_found.each do |row|
        begin
          if row == false
            not_found.close
          else

            email = row[0]
            begin

              # check the other lists
              mvt_contact = mvt_maropost_client.contacts.find_by_email(email: email) # triggers the not found exception
              row.push(mvt_contact["id"])
              LOGGER.info "#{email} found in mvt"

              mvr_contact = mvr_maropost_client.contacts.find_by_email(email: email) # triggers the not found exception
              row.push("",mvr_contact["id"])
              LOGGER.info "#{email} found in mvr"

            rescue MaropostApi::NotFound => ex
              LOGGER.info "#{email} not found in mvt or mvr"
            end

            csv << row
            csv.flush
            LOGGER.info "#{email} added to google sheets not found"
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
            csv << row
            csv.flush
            LOGGER.info "#{row[0]} added to google sheets do not mail"
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
