const key=fen=>String(fen||'').trim().split(/\s+/).slice(0,4).join(' ')
export function findPositionOccurrence(data, target) {
  if(!target)return {index:0,found:true}
  const index=(data.moves||[]).findIndex(move=>key(move.fen_after)===key(target.fen)
    &&(!target.n_move||move.move_number===target.n_move)
    &&(!target.move_color||move.move_color===target.move_color))
  if(index>=0)return {index:index+1,found:true}
  if(!target.n_move&&key(data.game?.initial_fen)===key(target.fen))return {index:0,found:true}
  return {index:0,found:false}
}
