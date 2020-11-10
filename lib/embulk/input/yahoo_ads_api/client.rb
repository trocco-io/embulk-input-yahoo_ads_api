require 'rest-client'
require 'date'
require 'securerandom'

module Embulk
  module Input
    module YahooAdsApi
      class Client
        def initialize(account_id, token)
          @account_id = account_id
          @token = token
        end

        def invoke(method, params)
          url = @base + method
          ::Embulk.logger.info "Access URI: #{url}"
          RestClient.post(
            url,
            params,
            {
              content_type: :json,
              accept: :json,
              Authorization: "bearer #{@token}"
            }).body
        end

        def temporarily_download(method, params)
          path = "./tmp/embulk-#{Date.today}/"
          FileUtils.mkdir_p(path)
          file_path = path+"#{SecureRandom.hex(10)}"
          File.open(file_path, 'w') {|f|
            block = proc { |response|
              response.read_body do |chunk|
                f.write chunk
              end
            }
            RestClient::Request.execute(
             method: :post,
             url: @base + method,
             payload: params,
             headers: {
               content_type: :json,
               accept: :json,
               Authorization: "bearer #{@token}"
             },
             block_response: block
            )
          }
          file_path
        end
      end
    end
  end
end
