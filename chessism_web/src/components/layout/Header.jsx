import { pageStyleTarget } from '../../appearance/styleCore.js'
function Header() {
  return (
    <header className="site-header">
      <div>
        <p {...pageStyleTarget('text')} className="eyebrow">Chess analytics workstation</p>
        <h1 {...pageStyleTarget('heading')}>Chessism</h1>
      </div>
    </header>
  )
}

export default Header
