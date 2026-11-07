// Disposable native-notification UI prototype; never imported by the plugin.
import { createServer } from 'node:http'
import { readFile } from 'node:fs/promises'

const html = await readFile(new URL('./notification-prototype.html', import.meta.url))
const server = createServer((request, response) => {
  if (process.env.NODE_ENV === 'production' || new URL(request.url, 'http://localhost').pathname !== '/notification-prototype') { response.writeHead(404); response.end(); return }
  response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' })
  response.end(html)
})
server.listen(5198, '127.0.0.1', () => console.log('Notification prototype: http://127.0.0.1:5198/notification-prototype?variant=B'))
