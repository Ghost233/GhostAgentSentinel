import { createServer } from 'node:http';
import { withModelSummary } from './model-summary.mjs';

export async function controlledModelService(t) {
  const server = createServer(async (request, response) => {
    let raw = ''; for await (const chunk of request) raw += chunk;
    const body = JSON.parse(raw);
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify(request.url === '/v1/chat/completions'
      ? { model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [], gaps: [] })) } }] }
      : { model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: 'unknown' }])) }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  const baseUrl = 'http://127.0.0.1:' + server.address().port;
  return { config: { llm: { baseUrl, model: 'controlled-summary' }, jev: { baseUrl, model: 'controlled-judge' } } };
}
