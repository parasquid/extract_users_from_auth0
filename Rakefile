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
require "progress_bar"

desc "extract all users from auth0"
task extract_users: :dotenv do
  LOGGER = Logger.new(STDOUT)
  LOGGER.level = Logger::WARN

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

  PER_PAGE = 100
  total_pages = (total_records / PER_PAGE).floor + 1

  Channel = Concurrent::Channel
  queue = Channel.new
  done = Channel.new(capacity: 1)
  worker_pool = Channel.new(capacity: 4)

  def worker(users, queue, pool, page)
    users.each do |user|
      Channel.go do
        row = [
          user["email"],
          user["email_verified"],
          user["given_name"],
          user["family_name"]
        ]
        queue << row
      end
    end
    LOGGER.debug ~pool
  end

  Channel.go {
    bar = ProgressBar.new(total_records)
    CSV.open("accounts.csv", "wb") do |csv|
      csv << ["email", "email_verified", "given_name", "family_name"]
      counter = 0
      while counter < total_records do
        row = ~queue
        csv <<  row
        counter += 1
        bar.increment!
      end
    end
    done << "all done!"
  }

  total_pages.times do |page|
    Channel.go do
      users = auth0.get_users(
        per_page: PER_PAGE,
        page: page,
        sort: "created_at:1",
        include_totals: true
      )["users"]
      worker(users, queue, worker_pool, page)
    end
    worker_pool << "done with this worker"
  end

  LOGGER.debug ~done

end

