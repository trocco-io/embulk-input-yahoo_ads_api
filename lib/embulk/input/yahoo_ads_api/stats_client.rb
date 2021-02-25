require "json"

module Embulk
  module Input
    module YahooAdsApi
      class StatsClient < Client
        STAT_COLUMNS = [
          { :request_name => "CAMPAIGN_ID", :api_name => "campaignId" },
          { :request_name => "CAMPAIGN_NAME", :api_name => "campaignName" },
          { :request_name => "ADGROUP_ID", :api_name => "adGroupId" },
          { :request_name => "ADGROUP_NAME", :api_name => "adGroupName" },
          { :request_name => "AD_ID", :api_name => "adId" },
          { :request_name => "AD_NAME", :api_name => "adName" },
          { :request_name => "IMPS", :api_name => "imps" },
          { :request_name => "IMPS_PREV", :api_name => "impsPrev" },
          { :request_name => "CLICK", :api_name => "clickCnt" },
          { :request_name => "CLICK_RATE", :api_name => "clickRate" },
          { :request_name => "CLICK_RATE_PREV", :api_name => "clickRatePrev" },
          { :request_name => "COST", :api_name => "cost" },
          { :request_name => "AVG_CPC", :api_name => "avgCpc" },
          { :request_name => "CONVERSIONS", :api_name => "conversions" },
          { :request_name => "CONV_RATE", :api_name => "conversionRate" },
          { :request_name => "CONVERSIONS_VIA_AD_CLICK", :api_name => "conversionsViaAdClick" },
          { :request_name => "CONVERSION_RATE_VIA_AD_CLICK", :api_name => "conversionRateViaAdClick" },
          { :request_name => "ALL_CONV", :api_name => "allConversions" },
          { :request_name => "ALL_CONV_RATE", :api_name => "allConversionRate" },
          { :request_name => "COST_PER_CONV", :api_name => "cpa" },
          { :request_name => "CONV_VALUE", :api_name => "conversionValue" },
          { :request_name => "VALUE_PER_CONV", :api_name => "valuePerConversions" },
          { :request_name => "CONV_VALUE_PER_COST", :api_name => "convValuePerCost" },
          { :request_name => "ALL_CONV_VALUE_PER_COST", :api_name => "allConvValuePerCost" },
          { :request_name => "CONV_VALUE_VIA_AD_CLICK_PER_COST", :api_name => "convValueViaAdClickPerCost" },
          { :request_name => "ALL_CONV_VALUE", :api_name => "allConversionValue" },
          { :request_name => "VALUE_PER_ALL_CONV", :api_name => "valuePerAllConversions" },
          { :request_name => "CONV_VALUE_VIA_AD_CLICK", :api_name => "conversionValueViaAdClick" },
          { :request_name => "VALUE_PER_CONV_VIA_AD_CLICK", :api_name => "valuePerConversionsViaAdClick" },
          { :request_name => "COST_PER_CONV_VIA_AD_CLICK", :api_name => "cpaViaAdClick" },
          { :request_name => "COST_PER_ALL_CONV", :api_name => "allCpa" },
          { :request_name => "CROSS_DEVICE_CONVERSIONS", :api_name => "crossDeviceConversions" },
          { :request_name => "AVG_DELIVER_RANK", :api_name => "avgDeliverRank" },
          { :request_name => "MEASURED_IMPS", :api_name => "measuredImps" },
          { :request_name => "TOTAL_VIEWABLE_IMPS", :api_name => "totalVimps" },
          { :request_name => "MEASURED_IMPS_RATE", :api_name => "measuredImpsRate" },
          { :request_name => "VIEWABLE_IMPS", :api_name => "vimps" },
          { :request_name => "VIEWABLE_IMPS_RATE", :api_name => "viewableImpsRate" },
          { :request_name => "INVIEW_RATE", :api_name => "inViewRate" },
          { :request_name => "VIEWABLE_CLICK", :api_name => "viewableClicks" },
          { :request_name => "INVIEW_CLICK", :api_name => "inViewClickCnt" },
          { :request_name => "VIEWABLE_CLICK_RATE", :api_name => "viewableClickRate" },
          { :request_name => "INVIEW_CLICK_RATE", :api_name => "inViewClickRate" },
          { :request_name => "PAID_VIDEO_VIEWS", :api_name => "paidVideoViews" },
          { :request_name => "PAID_VIDEO_VIEW_RATE", :api_name => "paidVideoViewRate" },
          { :request_name => "AVG_CPV", :api_name => "averageCpv" },
          { :request_name => "VIDEO_VIEWS", :api_name => "videoViews" },
          { :request_name => "VIDEO_VIEWS_TO_25", :api_name => "videoViewsTo25" },
          { :request_name => "VIDEO_VIEWS_TO_50", :api_name => "videoViewsTo50" },
          { :request_name => "VIDEO_VIEWS_TO_75", :api_name => "videoViewsTo75" },
          { :request_name => "VIDEO_VIEWS_TO_95", :api_name => "videoViewsTo95" },
          { :request_name => "VIDEO_VIEWS_TO_100", :api_name => "videoViewsTo100" },
          { :request_name => "VIDEO_VIEWS_TO_3_SEC", :api_name => "videoViewsTo3Sec" },
          { :request_name => "VIDEO_VIEWS_TO_10_SEC", :api_name => "videoViewsTo10Sec" },
          { :request_name => "AVG_PERCENT_VIDEO_VIEWED", :api_name => "averageRateVideoViewed" },
          { :request_name => "AVG_DURATION_VIDEO_VIEWED", :api_name => "averageDurationVideoViewed" },
          { :request_name => "IMPRESSION_SHARE", :api_name => "impressionShare" },
          { :request_name => "IMPRESSION_SHARE_BUDGET_LOSS", :api_name => "budgetImpressionShareLostRate" },
          { :request_name => "IMPRESSION_SHARE_RANK_LOSS", :api_name => "rankImpressionShareLostRate" },
        ].freeze

        def initialize(ads_type, account_id, token)
          super(account_id, token)
          @base = SERVERS[ads_type.to_sym] + "/StatsService/"
        end

        def run(query)
          raw_data = []
          get_stats(query, raw_data)
          ::Embulk.logger.info "Get Stats"
          reshape_data(raw_data, query)
        end

        def columns(_type = nil)
          STAT_COLUMNS
        end

        private

        def get_stats(config, raw_data, count = 1)
          number_result = 1
          get_config = {
            accountId: @account_id,
            statsPeriod: config[:date_range_type],
            periodCustomDate: {
              statsEndDate: config[:end_date],
              statsStartDate: config[:start_date],
            },
            targetTypes: [
              "GENDER_TARGET",
            ],
            type: config[:stats_type],
            startIndex: (count - 1) * number_result + 1,
            numberResults: number_result,
          }.to_json
          response = JSON.parse(self.invoke("get", get_config))

          if response.dig("rval", "values").nil?
            raw_data
          else
            if response["rval"]["values"][0]["operationSucceeded"] == false
              error = response["rval"]["values"][0]["errors"]
              raise ::Embulk::Input::YahooAdsApi::Error::InvalidEnumError, error.to_json
            else
              raw_data.concat(response["rval"]["values"])
              if response["rval"]["totalNumEntries"] > number_result * count
                get_stats(config, raw_data, count + 1)
              else
                raw_data
              end
            end
          end
        end

        def reshape_data(data, config)
          data_type = case config[:stats_type]
            when "CAMPAIGN"
              "campaignStatsValue"
            when "ADGROUP"
              "adGroupStatsValue"
            when "AD"
              "adStatsValue"
            end
          data.map do |value|
            stats_info = value[data_type]["stats"]
            value[data_type].delete("stats")
            stats_info.merge(value[data_type])
          end
        end
      end
    end
  end
end
