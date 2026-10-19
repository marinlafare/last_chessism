import Markdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import SideRail from '../components/layout/SideRail'
import Footer from '../components/layout/Footer'
import document from '@hegel-document?raw'
import './hegel/hegel.css'

export default function Hegel() {
  return (
    <div className="page-frame hegel-page">
      <SideRail />
      <div className="home-shell">
        <main className="hegel-main" aria-label="Hegel">
          <article className="hegel-document">
            <Markdown remarkPlugins={[remarkGfm]} skipHtml>{document}</Markdown>
          </article>
        </main>
        <Footer />
      </div>
    </div>
  )
}
