require "faraday"
require "manageiq-messaging"

module ReceptorController
  class Client
    require "receptor_controller/client/configuration"
    require "receptor_controller/client/exception"
    require "receptor_controller/client/response_worker"
    require "receptor_controller/client/directive_blocking"
    require "receptor_controller/client/directive_non_blocking"

    attr_accessor :default_headers, :identity_header, :logger, :response_worker
    attr_reader :config

    STATUS_DISCONNECTED = {'status' => 'disconnected'}.freeze

    class << self
      def configure
        if block_given?
          yield(Configuration.default)
        else
          Configuration.default
        end
      end
    end
    delegate :start, :stop, :to => :response_worker

    def initialize(config: Configuration.default, logger: ManageIQ::Messaging::NullLogger.new)
      self.config          = config
      self.default_headers = {"Content-Type" => "application/json"}
      self.logger          = logger
      self.response_worker = ResponseWorker.new(config, logger)
    end

    def connection_status(account_number, node_id)
      body = {
        :account => account_number,
        :node_id => node_id
      }.to_json

      response = Faraday.post(config.connection_status_url, body, headers)
      if response.success?
        JSON.parse(response.body)
      else
        logger.error(receptor_log_msg("Connection_status failed: HTTP #{response.status}", account_number, node_id))
        STATUS_DISCONNECTED
      end
    rescue Faraday::Error => e
      logger.error(receptor_log_msg("Connection_status failed", account_number, node_id, e))
      STATUS_DISCONNECTED
    end

    def directive(account_number, node_id,
                  payload:,
                  directive:,
                  type: :non_blocking)
      klass = type == :non_blocking ? DirectiveNonBlocking : DirectiveBlocking
      klass.new(:name    => directive,
                :account => account_number,
                :node_id => node_id,
                :payload => payload,
                :client  => self)
    end

    def headers
      default_headers.merge(identity_header || {})
    end

    def receptor_log_msg(msg, account, node_id, exception = nil)
      message = "Receptor: #{msg}"
      message += "; #{exception.class.name}: #{exception.message}" if exception.present?
      message + " [Account number: #{account}; Receptor node: #{node_id}]"
    end

    private

    attr_writer :config
  end
end
