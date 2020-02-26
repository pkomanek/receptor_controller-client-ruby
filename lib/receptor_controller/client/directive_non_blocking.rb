require "receptor_controller/client/directive"

module ReceptorController
  class Client::DirectiveNonBlocking < Client::Directive
    def call
      body = {
        :account   => account,
        :recipient => node_id,
        :payload   => payload,
        :directive => name
      }

      response = Faraday.post(config.job_url, body.to_json, client.headers)
      if response.success?
        msg_id = JSON.parse(response.body)['id']

        # registers message id for kafka responses
        response_worker.register_message(msg_id, self)

        msg_id
      else
        logger.error(receptor_log_msg("Directive #{name} failed: HTTP #{response.status}", account, node_id))
        nil
      end
    rescue Faraday::Error => e
      logger.error(receptor_log_msg("Directive #{name} failed", account, node_id, e))
      nil
    end
  end
end
