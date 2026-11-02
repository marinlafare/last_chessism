import { prepareLayout } from './openingMapLayout'
self.onmessage = ({ data }) => {
  try {
    const layout = prepareLayout(data, message => self.postMessage({ message }))
    self.postMessage({ layout })
  } catch (error) { self.postMessage({ error: error.message || 'Map layout failed.' }) }
}
