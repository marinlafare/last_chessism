import { pageStyleTarget } from '../../appearance/styleCore.js'
function Header({ eyebrow = 'Chess analytics workstation', title = 'Chessism' }) {
  return (
    <header className="site-header">
      <div>
        <p {...pageStyleTarget('text')} className="eyebrow">{eyebrow}</p>
        <h1 {...pageStyleTarget('heading')}>{title}</h1>
      </div>
    </header>
  )
}

export default Header
