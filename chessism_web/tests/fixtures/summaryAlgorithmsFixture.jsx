import React, { useState } from 'react'
import { createRoot } from 'react-dom/client'
import DefaultMatrices from '../../src/pages/research/matrices/DefaultMatrices'
import CudaValidation from '../../src/pages/research/algorithms/CudaValidation'
import BlunderTraining from '../../src/pages/research/algorithms/BlunderTraining'
import BlunderValidation from '../../src/pages/research/algorithms/BlunderValidation'
import BlunderV2 from '../../src/pages/research/algorithms/BlunderV2'
import AlgorithmResults from '../../src/pages/research/algorithms/AlgorithmResults'
import { runCudaValidation, trainBlunderModel, trainBlunderV2, validateFixedBlunders, fetchRun } from '../../src/pages/research/algorithms/algorithmApi'
import MatrixParametersForm from '../../src/pages/research/matrices/MatrixParametersForm'
import '../../src/pages/research/matrices/matricesConstructor.css'
import '../../src/pages/research/algorithms/algorithms.css'

window.summaryTest = { requests: [], report: {
  format:'chessism_cuda_comparison_v2', algorithm:'score-distribution', backend:'cuda_cupy',
  algorithm_version:2,input_contract:'score_distribution_v2',scored_only:true,
  status:'passed', canonical:false, rows:10, input_sha256:'a'.repeat(64), row_mismatches:0, counter_mismatches:0,
  groups:[], input_source:'synthetic', hardware:{name:'Fixture GPU'}, timings:{
    cpu_reference_seconds:.001,cuda_setup_seconds:.1,host_to_device_seconds:.001,
    calculation_seconds:.001,device_to_host_seconds:.001,total_warm_seconds:.003,
  },
} }
window.summaryTest.benchmark = {version:1,status:'passed',canonical:false,repeats:3,input_reuse:'within_run_only',database_extractions:1,
  fastest_measured_gpu_batch_rows:262144,wall_seconds:.5,
  variants:[8192,65536,262144].map((batch_rows,i)=>({batch_rows,
    validation:{status:'passed',rows:1000000,row_mismatches:0,counter_mismatches:0,input_sha256:'a'.repeat(64),peak_batch_rows:batch_rows},
    cuda_cupy_seconds:[.03-i*.01,.03-i*.01,.03-i*.01],cuda_cupy_median_seconds:.03-i*.01,
    numpy_cpu_seconds:[.05,.05,.05],numpy_cpu_median_seconds:.05}))}
const definitions = [
  ['score-distribution','Score distribution','score_distribution'],
  ['blunder-moves','Blunder moves — rating-aware inputs','blunder_moves'],
  ['blunder-fixed','Blunder moves — fixed coefficient','blunder_moves'],
].map(([key,name,row_type]) => ({id:'default:'+key,builtin_key:key,name,row_type,storage_kind:'definition',
  label_count:0,config:{row_type,definition_kind:'template',summary_input_version:2,
    feature_columns:row_type==='score_distribution'?['player','mode','player_score','score_kind','mate_moves']:key==='blunder-fixed'?
      ['player','game','mode','previous_player_score','previous_score_kind','player_score','score_kind','initial_prior']:
      ['player','game','mode','player_score','score_kind','analysis_source','previous_player_score','previous_score_kind','player_rating','opponent_rating','initial_prior'],label_columns:[]}}))
window.fetch = async (input, options={}) => {
  const path=new URL(input,location.href).pathname
  window.summaryTest.requests.push({path,method:options.method||'GET',body:options.body ? JSON.parse(options.body) : null})
  let data
  if(path.endsWith('/matrices/defaults')) data={definitions}
  else if(path.endsWith('/blunder-validation')) data={id:'fixed-blunder-fixture',status:'queued'}
  else if(path.endsWith('/runs/fixed-blunder-fixture')) data={name:'Fixed-coefficient blunders · CUDA validation · alice',status:'complete',result:{
    format:'chessism_fixed_blunder_validation_v1',status:'passed',canonical:false,production_changed:false,
    source:{player:'alice',games:10},rows:400,hardware:{name:'Fixture GPU'},rules:{coefficient:.00368208,probability_drop:.15},
    checks:{cpu_moves:0,sql_moves:0,cpu_games:0,sql_games:0,saved_games:0,saved_move_counts:0},
    groups:[{mode:'blitz',games:10,cpu:12,cuda:12,sql:12,saved:12}],unassessed_moves:2,
    timings:{metadata_seconds:.01,extraction_seconds:.1,sql_reference_seconds:.05,cpu_reference_seconds:.02,gpu_preflight_seconds:.1,cuda_warmup_seconds:.02,host_to_device_seconds:.001,gpu_calculation_seconds:.001,device_to_host_seconds:.001,validation_wall_seconds:.4},
    input_sha256:'f'.repeat(64),mismatch_examples:[],game_mismatch_examples:[],notes:['Only this research report is saved. No production publication was benchmarked.']}}
  else if(path.endsWith('/blunder-training-v2')) {window.summaryTest.refinement=JSON.parse(options.body).refinement;window.summaryTest.sampling=JSON.parse(options.body).sampling;data={id:'blunder-v2-fixture',status:'queued'}}
  else if(path.endsWith('/runs/blunder-v2-fixture')) data={name:'Rating-aware v2',status:'complete',result:{
    format:'chessism_rating_blunder_candidate_v2',source:{selected_games:1000,sampling_audit:{excluded_prior_games:27000,prior_game_overlap:0,
      cp_concentration:[{split:2,mode:'blitz',games:200,largest_player_share:.095,status:'passed'}]}},included_rows:40000,hardware:{name:'Fixture GPU'},
    cpu_cuda_passed:true,decision_mismatches:0,per_game_count_mismatches:0,gates:{numerical:'passed',coverage:'insufficient_data',production_recovery:'not_run'},
    fits:[{mode:'blitz',status:'fitted',a:.2,b:.3,regularization:.001,games_by_split:[700,150,100,50]}],
    blunder_comparison:[{mode:'blitz',old15:50,baseline20:40,candidate20:43}],
    calibration_diagnostics_version:1,
    test:{games:100,mse:.1,baseline_mse:.11,ece:.0609,baseline_ece:.08,ece_change:-.0191,
      calibration_uncertainty:{ece_change_ci95:[-.04,.01]},
      calibration:[{bin:9,predicted:.95,observed:.86,ece_contribution:.02}],
      baseline_calibration:[{bin:9,predicted:.94,observed:.84,ece_contribution:.03}]},
    held_out_players:{games:50,ece:.08,baseline_ece:.1,ece_change:-.02},strata:[],
    timings:{extraction_seconds:1,fitting_seconds:2,total_compute_seconds:4},notes:['Candidate only. No live changes.']}}
  else if(path.endsWith('/blunder-training')) data={id:'blunder-fixture',status:'queued'}
  else if(path.endsWith('/runs/blunder-fixture')) data={name:'Rating-aware blunders · CUDA candidate',status:'complete',result:{
    format:'chessism_rating_blunder_candidate_v1',fitted_groups:1,cpu_cuda_passed:true,source:{selected_games:1000},included_rows:40000,
    hardware:{name:'Fixture GPU'},timings:{extraction_seconds:1,fitting_and_reference_seconds:.5,held_out_comparison_seconds:.1},
    notes:['Live summaries unchanged.'],fits:[{group:0,rating_band:'<1200',mode:'bullet',status:'fitted',coefficient:.003,
      samples:{training:{games:700},validation:{games:150},test:{games:150}},metrics:{test:{expected_points_mse:.15,baseline_mse:.16}},blunder_comparison:{old_blunders:50,new_blunders:42}}]}}
  else if(path.endsWith('/preview')) data={columns:[{key:'player_score',role:'features',storage_dtype:'float64',encoding:'numeric'}],
    rows:[[50]],row_keys:['1:white:1:white'],shape:[1,1],dimensions:2,materialized:false,row_limit:50,total_rows:1}
  else if(path.endsWith('/cuda-validation')) {window.summaryTest.runBenchmark=JSON.parse(options.body).benchmark;data={id:'fixture-run',status:'queued'}}
  else if(path.endsWith('/runs/fixture-run')) data={name:'Score distribution · CUDA validation · alice',status:'complete',result:{
    ...window.summaryTest.report,input_source:'frozen_export',validation_scope:'live_player',rows:1000000,
    streaming_version:1,input_encoding:'score_distribution_le64_v1',peak_batch_rows:8192,
    ...(window.summaryTest.runBenchmark?{batch_benchmark:window.summaryTest.benchmark}:{}),
    source:{player:'alice',truncated:false,rows:1000000,expected_rows:1000000,extraction_version:2,temporary_input_bytes:32000000},
    reference_checks:{existing_sql:{status:'passed',mismatches:0},saved_cpu_summary:{status:'passed',mismatches:0}},
    timings:{...window.summaryTest.report.timings,gpu_preflight_seconds:.01,extraction_seconds:.01,sql_reference_seconds:.02,validation_wall_seconds:.2,stream_comparison_seconds:.1}}}
  else throw Error('Unmocked request: '+path)
  if(path.endsWith('/runs/blunder-v2-fixture') && window.summaryTest.refinement==='calibration_v1') {
    const originalTest = data.result.test
    const originalHeld = data.result.held_out_players
    data.result.refinement={test_evaluated:false,selection_status:'not_passed',development:{groups:[],phases:{}}}
    data.result.fits[0]={...data.result.fits[0],rating_strength:.5,validation_mse:.14,validation_ece:.045,
      selection:{status:'not_passed',eligible_candidates:0,baseline:{mse:.18,ece:.02}},
      refinement_trials:[{regularization:.03,rating_strength:.5,mse:.14,ece:.045,predictive_eligible:true,calibration_eligible:false,converged:true,search_boundary:false}]}
    data.result.test={status:'not_run'};data.result.held_out_players={status:'not_run'};data.result.blunder_comparison=[]
    data.result.gates.calibration_aware_selection='not_passed'
    if(window.summaryTest.sampling==='development_reuse_v1') Object.assign(data.result.source.sampling_audit,{
      reuse_policy:'development_reuse_v1',reused_training_games:700,reused_validation_games:120,
      excluded_protected_games:1000,test_prior_overlap:0,role_violations:0})
    if(window.summaryTest.finalEvaluation) {
      data.result.final_evaluation={test_started:true,test_completed:true,refitted:false}
      data.result.refinement.test_evaluated=true
      data.result.test=originalTest;data.result.held_out_players=originalHeld
    }
    if(window.summaryTest.finalReservation) data.result={format:'chessism_blunder_final_evaluation_reservation_v1',final_evaluation:{test_started:true}}
    if(window.summaryTest.shadow) data.result={format:'chessism_blunder_shadow_pilot_v1',source:{games:6000},checked_moves:200000,
      hardware:{name:'Fixture GPU'},smoke:{status:'passed'},status:'passed',max_prediction_error:1e-16,
      operational_checks:{incremental_equals_rebuild:true,duplicate_retry:{changed_games:0}},checks:{saved_game_mismatches:0,new_cpu_move_mismatches:0},
      groups:[{player:'*',mode:'rapid',games:1200,old15:300,baseline20:200,candidate20:190,unassessed:10,histograms:{old15:[1,2,3,4,5,6,7],baseline20:[2,3,4,5,6,7,8],candidate20:[3,4,5,6,7,8,9]}}],
      benchmark:{median_cpu_seconds:.02,median_gpu_seconds:.03},timings:{source_read_pack_seconds:2},
      examples:[{category:'rating_removed',player:'alice',mode:'rapid',game:1,ply:10,san:'Nf3',before:100,after:-100,before_kind:0,after_kind:0,old15:1,baseline20:1,candidate20:0,candidate_loss:.19,player_rating:1500,opponent_rating:1800}],
      original_quality_gates:{overall_calibration:'not_passed'},notes:['No live summaries changed.']}
    if(window.summaryTest.shadowCheckpoint) data.result={format:'chessism_blunder_shadow_checkpoint_v1',generation:1,records:[{}],groups:[]}
    if(window.summaryTest.thresholdComparison) {
      const group={player:'*',mode:'bullet',rating_band:'<1000',games:400,assessed_games:399,moves:10000,unassessed:20,
        old15:700,baseline20:500,candidate20:200,candidate15:350,changes:{restored_old:120,new_from_lowering:30,still_removed:380}}
      data.result={format:'chessism_blunder_threshold_comparison_v1',source:{games:6000},checked_moves:221312,hardware:{name:'Fixture GPU'},
        groups:[group],rating_groups:[group],parent_reference_checks:{saved_game_mismatches:0},checks:{threshold15_cpu_move_mismatches:0},
        examples:[{category:'restored_old',player:'alice',mode:'bullet',game:1,ply:7,san:'f4',before:492,after:-11,before_kind:0,after_kind:0,
          old15:1,candidate20:0,candidate15:1,candidate_loss:.153,player_rating:720,opponent_rating:663}],
        original_quality_gates:{overall_calibration:'not_passed'},timings:{total_compute_seconds:20},notes:['No live summaries changed.']}
    }
    if(window.summaryTest.cohort) {
      const g={games:10,assessed_games:9,moves:500,unassessed:10,old15:20,candidate20:10,candidate15:15,histograms:{candidate15:[2,3,2,1,1,0,0]}}
      const p={player:'alice',by_mode:{bullet:g},current_ui_by_mode:{bullet:{games:9,counts:[1,2,3,2,1,0,0]}},
        artifact:{relative_path:'blunder-cohorts/fixture/alice.jsonl.gz',records:10,sha256:'abc'}}
      data.result={format:window.summaryTest.cohortComplete?'chessism_blunder_cohort_report_v1':'chessism_blunder_cohort_checkpoint_v1',
        status:'passed',players:[p],inventory:[{player_name:'alice',scope:[{mode:'bullet',all_games:12,analyzed_games:11,unsupported_rating_games:1,nonstandard_games:0,eligible_games:10}]}],
        checks:{new_cpu_move:0},gpu_batches:[{rows:100,players:['alice','bob']}],by_mode:{bullet:g},rating_groups:[{rating_band:'<1000',mode:'bullet',...g}],
        original_quality_gates:{overall_calibration:'not_passed'},notes:['Live summaries unchanged.'],stale_players:[]}
    }
  }
  if(path.endsWith('/runs/blunder-v2-fixture') && window.summaryTest.strength) data.result={
    format:'chessism_blunder_strength_comparison_v1',source:{selected_games:100,selected_players:['alice','bob'],prior_game_overlap:0,selection:{}},
    hardware:{name:'Fixture RTX 4060'},included_rows:3000,candidates:[{id:'joint_50',label:'Both adjustments 50%'}],
    groups:[{kind:'mode',group:'bullet',candidate:'joint_50',games:100,old15:200,blunders:150,mse:.12,ece_pp:3.25,players:2,largest_owner_share:.5},
      {kind:'rating_mode',group:'<1000/bullet',candidate:'joint_50',games:25,old15:80,blunders:60,mse:.14,ece_pp:4,players:1,largest_owner_share:1}],
    examples:[{game:1,ply:3,player:'alice',mode:'bullet',san:'Nf3',before:0,after:-200,player_rating:800,opponent_rating:800,old15:1,candidates:{joint_50:{blunder:0,loss:.11}}}],
    gpu_batches:[{players:['alice','bob']}],checks:{decision_mismatches:0},max_prediction_error:1e-16,max_metric_error:1e-16,
    timings:{comparison_wall_seconds:2},original_quality_gates:{overall_calibration:'not_passed'},notes:['Live summaries unchanged.']}
  if(path.endsWith('/runs/blunder-v2-fixture') && window.summaryTest.strengthReservation) data.result={format:'chessism_blunder_strength_reservation_v1'}
  if(path.endsWith('/runs/blunder-v2-fixture') && window.summaryTest.fullDevelopment) data.result={
    format:window.summaryTest.fullReservation?'chessism_blunder_full_development_reservation_v1':'chessism_blunder_full_development_v1',
    source:{selected_games:300000,source_positions:22000000,selection:[{split:0,mode:'bullet',available_games:190000,selected_games:150000,largest_player_share:.1}]},
    hardware:{name:'NVIDIA GeForce RTX 4060'},gpu_memory:{pool_limit_bytes:7*1024**3},
    fits:[{mode:'bullet',selection:{status:'not_passed'},mse:.15,ece:.04,frozen_comparison:{ece:.05},training_batch_rows:1048576}],
    gates:{overall_calibration:'not_run',calibration_aware_selection:'not_passed'},original_quality_gates:{coverage:'insufficient_data'},notes:['Research only.'],
    development_diagnostics:{rapid:{groups:[{split:1,kind:'overall',group:'all',games:1682,models:{candidate:{mse:.142,ece_pp:4.62,bins:[{bin:5,game_weight:20,predicted:.55,observed:.6}]}}},
      {split:0,kind:'year',group:'2022',games:1000,models:{candidate:{mse:.14,ece_pp:3}}}],notes:['Training metrics are in-sample.']},
      bullet:{status:'passed',scalar_checked_moves:4096,groups:[{rating_band:'<1000',game_sides:100,blunders:{coefficient_only:505,frozen_reference:336,candidate:330},changes:{coefficient_only:{added:5,removed:180},frozen_reference:{added:2,removed:8}}}],examples:[],notes:['Development validation only.']}}}
  return new Response(JSON.stringify(data),{status:200,headers:{'Content-Type':'application/json'}})
}
function ValidationFixture() {
  const [run,setRun]=useState(null)
  const [busy,setBusy]=useState(false)
  const start=async (player,options)=>{setBusy(true);try{const queued=await runCudaValidation(player,options);setRun(await fetchRun(queued.id))}finally{setBusy(false)}}
  const train=async games=>{setBusy(true);try{const queued=await trainBlunderModel(games);setRun(await fetchRun(queued.id))}finally{setBusy(false)}}
  const smooth=async (games,sampling,refinement)=>{setBusy(true);try{const queued=await trainBlunderV2(games,sampling,refinement);setRun(await fetchRun(queued.id))}finally{setBusy(false)}}
  const fixed=async player=>{setBusy(true);try{const queued=await validateFixedBlunders(player);setRun(await fetchRun(queued.id))}finally{setBusy(false)}}
  return <><BlunderV2 onRun={smooth} disabled={busy}/><BlunderValidation onRun={fixed} disabled={busy}/><BlunderTraining onRun={train} disabled={busy}/><CudaValidation onRun={start} disabled={busy}/>{run && <AlgorithmResults run={run} onClose={()=>setRun(null)}/>}</>
}
createRoot(document.getElementById('root')).render(<main><DefaultMatrices/><ValidationFixture/>
  <MatrixParametersForm definition={definitions[0]} catalog={{limits:{},row_types:[{key:'score_distribution',filters:{players:true,modes:true,dates:true,analyzed_only:true}}]}}
    onApply={value=>{window.summaryTest.parameters=value}} />
</main>)
