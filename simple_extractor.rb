class SimpleExtractor
  PER_PAGE = 100
  attr_reader :total_records
  attr_reader :total_pages

  def initialize(q: nil, logger: Logger.new(STDOUT))
    @logger = logger

    @auth0 = init_auth0_client
    @total_records = @auth0.get_users(
      per_page: 1,
      sort: "created_at:1",
      include_totals: true,
      q: q
    )["total"]

    @q = q
    @total_pages = (@total_records / PER_PAGE).floor + 1
  end

  # get users from the API and push it to the queue
  def get_users_from_api(page: 0, q: @q)
    LOGGER.info "extractor processing page #{page} of #{@total_pages}"
    begin
      users = @auth0.get_users(
        per_page: @total_records < PER_PAGE ? @total_records : PER_PAGE,
        page: page,
        sort: "created_at:1",
        include_totals: true,
        q: q
      )["users"]

      json_to_array(users)
    rescue StandardError => ex
      @logger.warn ex
    end
  end

  def per_page
    PER_PAGE
  end

  def headers
    ["email", "email_verified", "given_name", "family_name", "created_at", "last_login", "logins_count", "user_id"]
  end

  private

  def json_to_array(users)
    queue = []
    users.each do |user|
      begin
        ident = user["identities"].select { |identity|
          identity["provider"] == "auth0"
        }.first
        row = [
          user["email"],
          user["email_verified"],
          user["given_name"],
          user["family_name"],
          user["created_at"],
          user["last_login"],
          user["logins_count"],
          (ident ? ident["user_id"] : "NO AUTH ID")
        ]
        queue << row
      rescue StandardError => ex
        @logger.warn ex
      end
    end
    queue
  end

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

end
