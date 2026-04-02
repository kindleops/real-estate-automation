import { child } from "@/lib/logging/logger.js";

const logger = child({
  module: "logging.events",
});

export function logSystemEvent(event, meta = {}) {
  logger.info(event, meta);
}

export function logQueueEvent(event, meta = {}) {
  logger.info(`queue.${event}`, meta);
}

export function logWebhookEvent(event, meta = {}) {
  logger.info(`webhook.${event}`, meta);
}

export function logFlowEvent(event, meta = {}) {
  logger.info(`flow.${event}`, meta);
}

export function logProviderEvent(event, meta = {}) {
  logger.info(`provider.${event}`, meta);
}

export function logDomainEvent(event, meta = {}) {
  logger.info(`domain.${event}`, meta);
}

export default {
  logSystemEvent,
  logQueueEvent,
  logWebhookEvent,
  logFlowEvent,
  logProviderEvent,
  logDomainEvent,
};