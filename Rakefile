require "dotenv/load"
require "dotenv/tasks"
require "pry"
require "auth0"
require "json"
require "jwt"
require 'uri'
require 'net/http'

desc "extract all emails from auth0"
task extract_emails: :dotenv do

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

  puts auth0.get_users
end

