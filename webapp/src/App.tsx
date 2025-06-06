import { createEffect, createSignal } from 'solid-js'
import './App.css'
import * as df from './pkg/df_audio_worklet'
import { CMAP_INFERNO } from './cmap';
import { fetchModel, setupAudioWorklet, writeWav } from './wasm-util';
import { Model } from './noise-model';

async function setupModelUpload() {
  const [wasm_df, modelTar] = await Promise.all([
    df.default(),
    fetchModel("/DeepFilterNet3_onnx.tar.gz"),
  ])

  const modelBytes = new Uint8Array(modelTar)

  return new Model(wasm_df, modelBytes, 1000)
}

function Loading() {
  const [dotNum, setDotNum] = createSignal<number>(0)

  setInterval(() => {
    setDotNum(dotNum() % 5 + 1)
  }, 400)

  return <>
    {
      "Loading " + ".".repeat(dotNum())
    }
  </>
}

function App() {
  const audioCtx = new AudioContext()
  const [file, setFile] = createSignal<File | null>(null);
  const [downloadUrl, setDownloadUrl] = createSignal<string>('');
  const [audioNode, setAudioNode] = createSignal<AudioWorkletNode>(null);
  const [loading, setLoading] = createSignal<boolean>(false);
  const [model, setModel] = createSignal<Model>(null);
  const [modelOutput, setModelOutput] = createSignal<AudioBuffer>(null);
  const [specVisualizer, setSpecVisualizer] = createSignal<
    {
      org: SpecVisualizer
      denoise: SpecVisualizer
    }
  >(null);

  //setupAudioWorklet("/DeepFilterNet3_onnx.tar.gz", audioCtx).then((node) => {
  //  setAudioNode(node)
  //  //node.port.postMessage({type: 'new'})
  //  //node.connect(audioCtx.destination)
  //  //setLoading(false)
  //})

  createEffect(() => {
    setLoading(true)
    setupModelUpload().then((model) => {
      setModel(model)
      setLoading(false)
    })
  })

  createEffect(() => {
    setSpecVisualizer(
      {
        org: get_visualizer('audio-canvas-org', audioCtx),
        denoise: get_visualizer('audio-canvas-denoise', audioCtx)
      }
    )
  })

  createEffect(() => {
    if (file() !== null) {
      specVisualizer().org.stop()
      specVisualizer().denoise.stop()
      const reader = new FileReader();
      setLoading(true)
      reader.addEventListener("load", async (event) => {
        const audioArray = event.target.result as ArrayBuffer;
        const audioBuffer = await audioCtx.decodeAudioData(audioArray)

        if (audioNode() !== null) {
          audioNode().connect(audioCtx.destination)
        }

        if (model() !== null) {
          let inputFloat = audioBuffer.getChannelData(0)

          const frameLength = model().frame_length()
          const fixToFrameLength = Math.ceil(inputFloat.length / frameLength) * frameLength
          let inputFrame = new Float32Array(fixToFrameLength)
          let outputFloat = new Float32Array(fixToFrameLength)
          inputFrame.set(inputFloat)

          let i = 0;
          while (i < inputFrame.length) {
            const frameOutput = model().process_frame(inputFrame.slice(i))
            //console.log(frameOutput.length)
            outputFloat.set(frameOutput, i)
            i += frameOutput.length
          }
          console.log(outputFloat)

          const outputAudio = audioCtx.createBuffer(2, outputFloat.length, audioBuffer.sampleRate)
          outputAudio.getChannelData(0).set(outputFloat)
          outputAudio.getChannelData(1).set(outputFloat)

          specVisualizer().org.visualize(audioBuffer, audioCtx, false)
          specVisualizer().denoise.visualize(outputAudio, audioCtx)
          setLoading(false)
          setModelOutput(outputAudio)
        }
      })
      reader.readAsArrayBuffer(file())
    }
  });

  createEffect(() => {
    if (modelOutput() !== null) {
      const data = writeWav(modelOutput())
      let blob = new Blob([data])
      let url = URL.createObjectURL(blob)
      setDownloadUrl(url)
    }
  })

  return (
    <>
      <div class="outer-box">
        <input
          type='file'
          disabled={loading()}
          onChange={(e) => {
            if (e.target.files !== null) {
              setFile(e.target.files[0])
            }
          }} />
        <div>
          {loading() && <Loading />}
        </div>
        {
          downloadUrl() !== '' &&
          <a href={downloadUrl()} download={file().name}>Download</a>
        }

        <div>
          <button onClick={() => {
            specVisualizer().org.play()
            specVisualizer().denoise.play()
          }}>Play</button>
          <button onClick={() => {
            specVisualizer().org.pause()
            specVisualizer().denoise.pause()
          }}>Stop</button>
        </div>

        <canvas id="audio-canvas-org" />
        <canvas id="audio-canvas-denoise" />
      </div>
    </>
  )
}

function get_visualizer(canvasId: string, audioCtx: AudioContext) {
  const canvas = document.getElementById(canvasId) as HTMLCanvasElement

  canvas.width = 300
  canvas.height = 200
  const analyzer = audioCtx.createAnalyser()
  analyzer.fftSize = 256

  return new SpecVisualizer(canvas, 100, analyzer)
}

class SpecVisualizer {
  canvas: HTMLCanvasElement
  canvasCtx: CanvasRenderingContext2D
  barHeight: number
  barWidth: number
  window_size: number
  analyser: AnalyserNode
  specs: RingBuf<Uint8Array>
  frequencyData: Uint8Array
  drawLoop: number
  source: AudioBufferSourceNode

  constructor(canvas: HTMLCanvasElement, window_size: number, analyser: AnalyserNode) {
    this.canvas = canvas
    this.canvasCtx = canvas.getContext('2d')
    this.window_size = window_size
    this.barHeight = this.canvas.height / analyser.frequencyBinCount
    this.barWidth = this.canvas.width / window_size
    this.frequencyData = new Uint8Array(analyser.frequencyBinCount)
    this.analyser = analyser

    this.source = null
    this.specs = new RingBuf(window_size, () => new Uint8Array(analyser.frequencyBinCount))
  }

  visualize(audioBuffer: AudioBuffer, audioCtx: AudioContext, sound: boolean = true) {
    this.source = audioCtx.createBufferSource()
    this.source.buffer = audioBuffer // register audio source
    this.source.connect(this.analyser)
    this.source.start()

    this.analyser.connect(audioCtx.destination)

    this.drawLoop = setInterval(() => {
      this.update()
    }, 50)
  }

  pause() {
    this.source.stop()
  }

  play() {
    //this.source.channelInterpretation
  }

  stop() {
    clearInterval(this.drawLoop)
    this.analyser.disconnect()
    if (this.source !== null) {
      this.source.disconnect()
    }
  }

  update() {
    this.analyser.getByteFrequencyData(this.frequencyData)
    this.specs.push_update((array) => { array.set(this.frequencyData) })

    let specView = new Array<Uint8Array>(this.window_size)
    this.specs.view(specView, 0)

    this.canvasCtx.clearRect(0, 0, this.canvas.width, this.canvas.height)
    for (let i = 0; i < specView.length - 1; i++) {
      let freqData = specView[i]
      for (let j = 0; j < freqData.length; j++) {
        this.canvasCtx.fillStyle = CMAP_INFERNO[freqData[j]]
        this.canvasCtx.fillRect(
          i * this.barWidth,
          (freqData.length - j - 1) * this.barHeight,
          this.barWidth,
          this.barHeight
        )
      }
    }
  }

}

class RingBuf<T> {
  bufInner: Array<T>
  start: number
  index: number
  constructor(length: number, defaultValue: () => T) {
    this.bufInner = Array.from({ length }, defaultValue)
    //(length, default)
    this.index = 0
    this.start = 0
  }

  push_update(fn: (value: T) => void) {
    fn(this.bufInner[this.index])
    this.index++
    if (this.index >= this.bufInner.length) {
      this.index = 0
      this.start++
    } else if (this.start >= this.index) {
      this.start++
    }

    if (this.start === this.bufInner.length) {
      this.start = 0
    }
  }

  push(value: T) {
    this.bufInner[this.index] = value
    this.index++
    if (this.index >= this.bufInner.length) {
      this.index = 0
      this.start++
    } else if (this.start >= this.index) {
      this.start++
    }

    if (this.start === this.bufInner.length) {
      this.start = 0
    }
  }

  view(buf: Array<T>, startIdx: number = 0) {
    let i = 0;
    const bufLength = buf.length
    let start = (this.start + startIdx) % bufLength

    while (i < bufLength) {
      const popOut = Math.min(this.bufInner.length - start, bufLength - i)
      buf.splice(i, popOut, ...this.bufInner.slice(start, start + popOut))
      i += popOut
      start += popOut

      if (start >= this.bufInner.length) {
        start = start - this.bufInner.length
      }
    }

    console.assert(bufLength === buf.length)
  }

  len() {
    return this.bufInner.length
  }

}

export default App



