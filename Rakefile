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

desc "extract all users from auth0"
task extract_users: :dotenv do
  LOGGER = Logger.new(STDOUT)
  LOGGER.level = Logger::DEBUG

  url = URI("https://#{ENV['SUBDOMAIN']}.auth0.com/oauth/token")

  http = Net::HTTP.new(url.host, url.port)
  http.use_ssl = true
  http.verify_mode = OpenSSL::SSL::VERIFY_NONE

  request = Net::HTTP::Post.new(url)
  request["content-type"] = 'application/json'
  request_template = '{"client_id":"%{client_id}","client_secret":"%{client_secret}","audience":"https://%{subdomain}.auth0.com/api/v2/","grant_type":"client_credentials"}'
  request.body = request_template % {
    client_id: ENV["CLIENT_ID"],
    client_secret: ENV["CLIENT_SECRET"],
    subdomain: ENV["SUBDOMAIN"],
  }

  response = http.request(request)
  token = JSON.parse response.read_body

  auth0 = Auth0Client.new(
    :client_id => ENV["CLIENT_ID"],
    :token => token["access_token"],
    :domain => "#{ENV['SUBDOMAIN']}.auth0.com",
    :api_version => 2
  )

  total_records = auth0.get_users(
    per_page: 1,
    sort: "created_at:1",
    include_totals: true
  )["total"]

  total_records = 1000
  PER_PAGE = 100
  total_pages = (total_records / PER_PAGE).floor + 1

  Channel = Concurrent::Channel
  queue = Channel.new
  done = Channel.new(capacity: 1)
  worker_pool = Channel.new(capacity: 8)

  # get users from the API and push it to a worker
  total_pages.times do |page|
    Channel.go do
      begin
        users = auth0.get_users(
          per_page: PER_PAGE,
          page: page,
          sort: "created_at:1",
          include_totals: true
        )["users"]
        transform_users_json_to_hash(users, queue, worker_pool)
      rescue StandardError => ex
        LOGGER.warn ex
      end
    end
    worker_pool << "done with this worker"
  end

  # write each user to a csv
  Channel.go {
    begin
      LOGGER.debug "starting writer channel"
      write_user_row_to_csv(queue, total_records, done)
    rescue StandardError => ex
      LOGGER.warn ex
    end
  }

  LOGGER.debug ~done

end

# worker to parse the api response and spit out something usable
def transform_users_json_to_hash(users, queue, pool)
  users.each do |user|
    Channel.go do
      row = [
        user["email"],
        user["email_verified"],
        user["given_name"],
        user["family_name"],
        user["identities"].select { |identity|
          identity["provider"] == "auth0"
        }.first["user_id"]
      ]
      queue << row
    end
  end
  LOGGER.debug ~pool
end

def write_user_row_to_csv(users_queue, total_records, done_queue)
  LOGGER.debug "opening the csv for writing"
  CSV.open("accounts.csv", "wb") do |csv|
    csv << ["email", "email_verified", "given_name", "family_name"]
    counter = 0
    LOGGER.debug "reading the queue #{total_records} total records"
    while counter < total_records do
      row = ~users_queue
      csv <<  row
      counter += 1
    end
  end
  done_queue << "all done!"
end
