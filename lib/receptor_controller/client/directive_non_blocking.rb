require "receptor_controller/client/directive"

module ReceptorController
  class Client::DirectiveNonBlocking < Client::Directive
    def call(body = default_body)
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

    def on_success(&block)
      @on_response ||= []
      @on_response << block if block_given?
      @on_response
    end

    def on_eof(&block)
      @on_eof ||= []
      @on_eof << block if block_given?
      @on_eof
    end

    def on_timeout(&block)
      @on_timeout ||= []
      @on_timeout << block if block_given?
      @on_timeout
    end

    def on_error(&block)
      @on_error ||= []
      @on_error << block if block_given?
      @on_error
    end

    def response_success(msg_id, message_type, response)
      if message_type == MESSAGE_TYPE_EOF
        on_eof.each { |block| block.call(msg_id) }
      else
        on_success.each { |block| block.call(msg_id, response) }
      end
    end

    def response_error(msg_id, response_code)
      on_error.each { |block| block.call(msg_id, response_code) }
    end

    def response_timeout(msg_id)
      on_timeout.each { |block| block.call(msg_id) }
    end
  end
end
