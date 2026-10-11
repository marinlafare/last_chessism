import { getJson } from '../../services/apiClient'
export const fetchPositionExamples=({scope,token,page=0,signal})=>getJson(`/fens/explore?${new URLSearchParams({scope,page:String(page),...(token?{token}:{})})}`,{signal})
export const fetchPositionGames=({fen,after=0,signal})=>getJson(`/fens/explore/games?${new URLSearchParams({fen,after:String(after)})}`,{signal})
