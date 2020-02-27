module ReceptorController
  class Client::Configuration
    attr_reader :controller_scheme
    attr_reader :controller_host

    attr_accessor :connection_status_path
    attr_accessor :job_path

    attr_accessor :queue_host
    attr_accessor :queue_max_bytes
    attr_accessor :queue_persist_ref
    attr_accessor :queue_port
    attr_accessor :queue_topic

    attr_accessor :response_timeout
    attr_accessor :response_timeout_poll_time

    def initialize
      @controller_scheme = 'http'
      @controller_host   = 'localhost:9090'

      @connection_status_path = '/connection/status'
      @job_path               = '/job'

      @queue_host        = nil
      @queue_max_bytes   = nil
      @queue_persist_ref = nil
      @queue_port        = nil
      @queue_topic       = 'platform.receptor-controller.responses'

      @response_timeout           = 2.minutes
      @response_timeout_poll_time = 10.seconds # TODO: use condition variable?

      yield(self) if block_given?
    end

    def self.default
      @@default ||= new
    end

    def configure
      yield(self) if block_given?
    end

    def controller_scheme=(scheme)
      # remove :// from scheme
      @controller_scheme = scheme.sub(/:\/\//, '')
    end

    def controller_host=(host)
      # remove http(s):// and anything after a slash
      @controller_host = host.sub(/https?:\/\//, '').split('/').first
    end

    def controller_url
      "#{controller_scheme}://#{controller_host}".sub(/\/+\z/, '')
    end

    def connection_status_url
      File.join(controller_url, connection_status_path)
    end

    def job_url
      File.join(controller_url, job_path)
    end
  end
end
