class Extractor
  PER_PAGE = 100

  attr_reader :total_records

  def initialize(queue_channel, done_channel, logger: Logger.new(STDOUT))
    @queue = queue_channel
    @done = done_channel
    @logger = logger

    @worker_pool = Concurrent::Channel.new(capacity: 8)
    @auth0 = init_auth0_client
    @total_records = @auth0.get_users(
      per_page: 1,
      sort: "created_at:1",
      include_totals: true
    )["total"]

    @total_records = 1000
    @total_pages = (@total_records / PER_PAGE).floor + 1
  end

  # get users from the API and push it to a worker
  def get_users_from_api
    @total_pages.times do |page|
      Channel.go do
        begin
          users = @auth0.get_users(
            per_page: PER_PAGE,
            page: page,
            sort: "created_at:1",
            include_totals: true
          )["users"]
          transform_users_json_to_hash(users, @queue, @worker_pool)
        rescue StandardError => ex
          @logger.warn ex
        end
      end
      @worker_pool << "done with this worker"
    end
  end

  # write each user to a csv
  def write_user_row_to_csv
    @logger.debug "opening the csv for writing"
    counter = 0
    begin
      CSV.open("accounts.csv", "wb") do |csv|
        csv << ["email", "email_verified", "given_name", "family_name"]
        @logger.debug "reading the queue #{total_records} total records"
        while counter < @total_records do
          row = ~@queue
          csv <<  row
          @logger.debug "#{counter} of #{@total_records} processed"
          counter += 1
        end
      end
    rescue StandardError => ex
      @logger.warn ex
    end

    @done << "all done!"
  end

  private

  def init_auth0_client
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

    Auth0Client.new(
      :client_id => ENV["CLIENT_ID"],
      :token => token["access_token"],
      :domain => "#{ENV['SUBDOMAIN']}.auth0.com",
      :api_version => 2
    )
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
    @logger.debug ~pool
  end

end
