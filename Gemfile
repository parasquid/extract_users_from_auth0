ruby "~> 2.3"

source 'https://rails-assets.org' do
  gem 'rails-assets-tether', '>= 1.1.0'
  gem 'rails-assets-fontawesome', '~> 4.3.0'
end

source 'https://rubygems.org'

git_source(:github) do |repo_name|
  repo_name = "#{repo_name}/#{repo_name}" unless repo_name.include?("/")
  "https://github.com/#{repo_name}.git"
end

group :development, :test do
  # Use sqlite3 as the database for Active Record
  gem "pry"
  gem "byebug"
  gem "dotenv"
end

gem "naught"
gem "auth0"
gem "concurrent-ruby"
gem "concurrent-ruby-edge"
gem "parallel"
gem "jwt"

gem "maropost_api"
gem "g_sheets"
