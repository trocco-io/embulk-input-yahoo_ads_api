$:.push File.expand_path("../lib/embulk/input", __FILE__)
require "yahoo_ads_api/version"

Gem::Specification.new do |spec|
  spec.name          = "embulk-input-yahoo_ads_api"
  spec.version       = Embulk::Input::YahooAdsApi::VERSION
  spec.authors       = ["kazuki-yane"]
  spec.summary       = "Yahoo Ads Api input plugin for Embulk"
  spec.description   = "Loads records from Yahoo Ads Api."
  spec.email         = ["yanekazuki@yahoo.co.jp"]
  spec.licenses      = ["MIT"]
  spec.homepage      = "https://github.com/yanekazuki/embulk-input-yahoo_ads_api"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'nokogiri', ['~> 1.8.1']
  spec.add_dependency 'rest-client', ['~> 2.1.0']

  spec.add_development_dependency 'embulk', ['>= 0.9.8']
  spec.add_development_dependency 'bundler', ['>= 1.10.6']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
