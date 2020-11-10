Dir[File.join(__dir__, 'yahoo_ads_api', '**/*.rb')].each {|f| require f }

module Embulk
  module Input
    module YahooAdsApi
    end
  end
end
