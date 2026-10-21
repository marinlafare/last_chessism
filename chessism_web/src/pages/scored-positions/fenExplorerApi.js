import { getJson } from '../../services/apiClient'
export const fetchPositionExamples=({scope,token,page=0,signal})=>getJson(`/fens/explore?${new URLSearchParams({scope,page:String(page),include_details:'false',...(token?{token}:{})})}`,{signal})
