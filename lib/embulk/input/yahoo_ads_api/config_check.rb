module Embulk
  module Input
    module YahooAdsApi
      class ConfigCheck
        def self.check(config)
          if config["target"] != "report" && config["target"] != "stats"
            raise ::Embulk::Input::YahooAdsApi::Error::WrongConfigError, "Invalid value in target."
          end
          if config["target"] == "stats" && config["ads_type"] == "yss"
            raise ::Embulk::Input::YahooAdsApi::Error::WrongConfigError, "Stats is not supported in YSS."
          end
          if (config["target"] == "stats" || config["ads_type"] == "yss") && config["report_type"].nil?
            raise ::Embulk::Input::YahooAdsApi::Error::WrongConfigError, "report_type must be filled."
          end
        end
      end
    end
  end
end
