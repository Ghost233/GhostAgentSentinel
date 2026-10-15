import { sanitizeObservation } from './privacy.mjs';

function metadata(value, field, max = 500) {
  if (typeof value !== 'string' || !value.trim() || value.length > max || /[\u0000-\u001f]/.test(value)) throw new Error(`${field} must be a nonempty metadata string (max ${max})`);
  if (sanitizeObservation(value).redacted) throw new Error(`${field} must not contain credentials`);
  return value;
}

// Installation metadata is separate from the 24-hour observation history.
export function normalizeEnrollment(entries) {
  if (!Array.isArray(entries) || entries.length > 100) throw new Error('sources must be a bounded list (max 100)');
  const seen = new Set();
  return entries.map(input => {
    if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('source configuration must be an object');
    if (Object.keys(input).some(key => !['registrationId', 'name', 'profile', 'enabled'].includes(key))) throw new Error('unsupported source configuration field');
    const registrationId = metadata(input.registrationId, 'registrationId');
    if (seen.has(registrationId)) throw new Error('source configuration registrationId must be unique');
    seen.add(registrationId);
    if (typeof input.enabled !== 'boolean') throw new Error('source configuration enabled must be boolean');
    const result = { registrationId, name: metadata(input.name, 'name'), enabled: input.enabled };
    if (input.profile !== undefined) result.profile = metadata(input.profile, 'profile', 2000);
    return result;
  });
}
