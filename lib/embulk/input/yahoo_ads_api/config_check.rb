module Embulk
  module Input
    module YahooAdsApi
      class ConfigCheck
        def self.check(config)
          if config["target"] != "report" && config["target"] != "stats"
            raise ::Embulk::Input::YahooAdsApi::Error::WrongConfigError, "Invalid value in target."
          end
          if config["target"] == "stats" && config["servers"].include?("search")
            raise ::Embulk::Input::YahooAdsApi::Error::WrongConfigError, "Stats is not supported in YSS."
          end
          if (config["target"] == "stats" || config["servers"].include?("search")) && config["report_type"].nil?
            raise ::Embulk::Input::YahooAdsApi::Error::WrongConfigError, "report_type must be filled."
          end
        end
      end
    end
  end
end
  