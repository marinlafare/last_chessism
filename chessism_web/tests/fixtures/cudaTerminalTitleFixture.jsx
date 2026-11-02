import React from 'react'
import { createRoot } from 'react-dom/client'
import CudaStepTitle from '../../src/pages/positions/CudaStepTitle.jsx'
import '../../src/pages/positions/cuda-summaries.css'
history.replaceState(null, '', '/analize_positions')
window.fetch = () => { throw Error('This fixture makes no API requests') }
const root = createRoot(document.getElementById('root'))
window.cudaTitleTest = { title: 'Preparing CUDA inputs', active: true,
  render() { root.render(<CudaStepTitle key={this.title} title={this.title} active={this.active} />) } }
window.cudaTitleTest.render()
