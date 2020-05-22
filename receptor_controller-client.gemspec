# -*- encoding: utf-8 -*-

=begin
# Receptor-Controller Client
=end

$:.push File.expand_path("../lib", __FILE__)
require "receptor_controller/client/version"

Gem::Specification.new do |s|
  s.name        = "receptor_controller-client"
  s.version     = ReceptorController::Client::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Martin Slemr"]
  s.email       = ["mslemr@redhat.com"]
  s.homepage    = "https://github.com/RedHatInsights/receptor_controller-client-ruby"
  s.summary     = "Client for communication with Platform Receptor Controller - Gem"
  s.description = "Client for communication with Platform Receptor Controller"
  s.license     = "Apache-2.0"
  s.required_ruby_version = ">= 2.0"

  s.add_runtime_dependency 'activesupport', '~> 5.2.4.3'
  s.add_runtime_dependency 'concurrent-ruby', '~> 1.1', '>= 1.1.6'
  s.add_runtime_dependency 'faraday', '~> 1.0'
  s.add_runtime_dependency 'json', '~> 2.3', '>= 2.3.0'
  s.add_runtime_dependency 'manageiq-loggers', '~> 0.4.0', '>= 0.4.2'
  s.add_runtime_dependency 'manageiq-messaging', '~> 0.1.5'

  s.add_development_dependency 'bundler', '~> 2.0'
  s.add_development_dependency 'rspec', '~> 3.6', '>= 3.6.0'
  s.add_development_dependency 'webmock', '~> 1.24', '>= 1.24.3'
  s.add_development_dependency 'simplecov', '~> 0.17.1'
  s.add_development_dependency "rake", ">= 12.3.3"

  s.files         = `find *`.split("\n").uniq.sort.select { |f| !f.empty? }
  s.test_files    = `find spec/*`.split("\n")
  s.executables   = []
  s.require_paths = ["lib"]
end
