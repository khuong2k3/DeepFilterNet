import { createSignal } from 'solid-js'
import solidLogo from './assets/solid.svg'
import viteLogo from '/vite.svg'
import * as wasm from "./pkg/df_bg"
//import wasmUrl from "./pkg/df_bg.wasm?url"
import './App.css'

//async function loadWasm() {
//
//
//}

function App() {
  const [file, setFile] = createSignal<File | null>(null);
  //const resolveUrl = ()

  console.log(wasm)
  //console.log(wasmUrl)
  return (
    <>
      <input type='file' />
    </>
  )
}

export default App
