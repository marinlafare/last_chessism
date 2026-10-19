import { pageStyleTarget } from '../../appearance/styleCore.js'
function Footer() {
  return (
    <footer className="site-footer">
      <p {...pageStyleTarget('text')} className="footer-note">Local Chessism stack</p>
      <div className="footer-links">
        <a {...pageStyleTarget('link')} href="/api/docs">Documentation</a>
      </div>
    </footer>
  )
}

export default Footer
