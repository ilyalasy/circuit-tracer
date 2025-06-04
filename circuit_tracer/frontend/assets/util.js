window.util = (function () {
  var params = (function(){
    var rv = {}

    rv.get = key => {
      var url = new URL(window.location)
      var searchParams = new URLSearchParams(url.search)

      var str = searchParams.get(key)
      return str && decodeURIComponent(str)
    }

    rv.getAll = () => {
      var url = new URL(window.location)
      var searchParams = new URLSearchParams(url.search)

      var values = {}
      for (const [key, value] of searchParams.entries()) {
        values[key] = decodeURIComponent(value)
      }
      return values
    }

    rv.set = (key, value) => {
      var url = new URL(window.location)
      var searchParams = new URLSearchParams(url.search)

      if (value === null) {
        searchParams.delete(key)
      } else {
        searchParams.set(key, value)
      }

      url.search = searchParams.toString()
      history.replaceState(null, '', url)
    }

    return rv
  })()
  
  async function streamParseJSON(response) {
    // For very large JSON files, stream directly to IndexedDB without loading everything into memory
    
    if (!response.body) {
      // Fallback for browsers that don't support streams
      console.warn('Streaming not supported, falling back to regular parsing')
      return await response.json()
    }
    
    console.log('Starting direct-to-IndexedDB streaming...')
    var startTime = Date.now()
    
    try {
      // Extract slug from URL for caching
      const url = response.url || ''
      const slug = url.split('/').pop()?.replace('.json', '') || 'unknown'
      
      // Add progress indicator to page
      var progressEl = document.createElement('div')
      progressEl.style.cssText = `
        position: fixed; top: 20px; right: 20px; 
        background: rgba(0,0,0,0.8); color: white; 
        padding: 10px; border-radius: 5px; 
        font-family: monospace; z-index: 9999;
      `
      progressEl.textContent = 'Streaming large graph data to cache...'
      document.body.appendChild(progressEl)
      
      const result = await streamJSONToIndexedDB(response, slug, progressEl)
      
      var elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
      progressEl.textContent = `Cached to IndexedDB in ${elapsed}s. Loading subset...`
      
      // Load progressive subset from IndexedDB
      const progressiveData = await loadGraphDataProgressive(slug, 5000, 10000)
      
      // Remove progress indicator after 3 seconds
      setTimeout(() => {
        if (progressEl.parentNode) {
          progressEl.parentNode.removeChild(progressEl)
        }
      }, 3000)
      
      console.log(`Streaming complete in ${elapsed}s`)
      return progressiveData || result
      
    } catch (error) {
      console.error('Error in streaming JSON parser:', error)
      
      // Remove progress indicator on error
      var progressEl = document.querySelector('div[style*="position: fixed"]')
      if (progressEl && progressEl.parentNode) {
        progressEl.parentNode.removeChild(progressEl)
      }
      
      // Fallback to regular parsing if streaming fails
      console.log('Falling back to regular JSON parsing...')
      return await response.json()
    }
  }
  
  async function streamJSONToIndexedDB(response, slug, progressEl) {
    // Stream and parse JSON directly to IndexedDB without holding full data in memory
    
    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    let totalSize = 0
    let lastProgress = 0
    
    // State for incremental JSON parsing
    let depth = 0
    let inString = false
    let escapeNext = false
    let currentKey = ''
    let parsingState = 'root'
    let bracketCount = 0
    let currentObject = ''
    let currentObjectKey = ''
    
    // Parsed data
    let metadata = null
    let qParams = null
    let nodesProcessed = 0
    let linksProcessed = 0
    let nodeChunks = []
    let linkChunks = []
    const chunkSize = 1000
    
    // Open IndexedDB
    const db = await openGraphDB()
    
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      
      const chunk = decoder.decode(value, { stream: true })
      buffer += chunk
      totalSize += value.length
      
      // Update progress every 10MB
      if (totalSize - lastProgress > 10 * 1024 * 1024) {
        lastProgress = totalSize
        progressEl.textContent = `Streaming ${(totalSize / 1024 / 1024).toFixed(1)} MB to cache...`
        
        // Yield control to browser
        await new Promise(resolve => setTimeout(resolve, 1))
      }
      
      // Process buffer for complete JSON objects
      await processBuffer()
    }
    
    // Process any remaining buffer
    await processBuffer(true)
    
    // Save any remaining chunks
    await saveRemainingChunks()
    
    // Save metadata
    await saveMetadata()
    
    db.close()
    
    return {
      metadata,
      qParams,
      nodes: [],
      links: [],
      _partial: true,
      _totalNodes: nodesProcessed,
      _totalLinks: linksProcessed,
      _loadedNodes: 0,
      _loadedLinks: 0
    }
    
    async function processBuffer(final = false) {
      // Simple incremental JSON parsing - look for complete objects
      let processed = 0
      
      while (processed < buffer.length) {
        const char = buffer[processed]
        
        if (inString) {
          if (escapeNext) {
            escapeNext = false
          } else if (char === '\\') {
            escapeNext = true
          } else if (char === '"') {
            inString = false
          }
        } else {
          if (char === '"') {
            inString = true
          } else if (char === '{') {
            depth++
            if (parsingState === 'in_nodes' || parsingState === 'in_links') {
              bracketCount++
              if (bracketCount === 1) {
                currentObject = '{'
              } else {
                currentObject += char
              }
            }
          } else if (char === '}') {
            depth--
            if (parsingState === 'in_nodes' || parsingState === 'in_links') {
              currentObject += char
              bracketCount--
              
              if (bracketCount === 0) {
                // Complete object found
                await processCompleteObject(currentObject, parsingState)
                currentObject = ''
              }
            }
          } else if ((parsingState === 'in_nodes' || parsingState === 'in_links') && bracketCount > 0) {
            currentObject += char
          }
          
          // Check for section transitions
          if (!inString && char === ':') {
            const beforeColon = buffer.substring(Math.max(0, processed - 20), processed).match(/"([^"]+)"\s*$/)?.[1]
            if (beforeColon === 'metadata' && !metadata) {
              parsingState = 'metadata'
            } else if (beforeColon === 'qParams' && !qParams) {
              parsingState = 'qParams'  
            } else if (beforeColon === 'nodes') {
              parsingState = 'in_nodes'
              bracketCount = 0
            } else if (beforeColon === 'links') {
              parsingState = 'in_links'
              bracketCount = 0
            }
          }
        }
        
        processed++
        
        // Yield control periodically
        if (processed % 10000 === 0) {
          await new Promise(resolve => setTimeout(resolve, 0))
        }
      }
      
      // Keep unprocessed part of buffer
      if (!final && processed > 1000) {
        buffer = buffer.substring(processed - 1000) // Keep some overlap
      }
    }
    
    async function processCompleteObject(objStr, type) {
      try {
        const obj = JSON.parse(objStr)
        
        if (type === 'in_nodes') {
          nodeChunks.push(obj)
          nodesProcessed++
          
          if (nodeChunks.length >= chunkSize) {
            await saveNodeChunk()
          }
        } else if (type === 'in_links') {
          linkChunks.push(obj)
          linksProcessed++
          
          if (linkChunks.length >= chunkSize) {
            await saveLinkChunk()
          }
        }
        
        // Update progress
        if ((nodesProcessed + linksProcessed) % 5000 === 0) {
          progressEl.textContent = `Processed ${nodesProcessed} nodes, ${linksProcessed} links...`
        }
        
      } catch (e) {
        console.warn('Failed to parse object:', objStr.substring(0, 100))
      }
    }
    
    async function saveNodeChunk() {
      if (nodeChunks.length === 0) return
      
      // Sort by influence before saving
      const sortedNodes = [...nodeChunks].sort((a, b) => (b.influence || 0) - (a.influence || 0))
      
      const chunkIndex = Math.floor((nodesProcessed - nodeChunks.length) / chunkSize)
      const chunk = {
        id: `${slug}_nodes_${chunkIndex}`,
        slug,
        chunkIndex,
        type: 'nodes',
        data: sortedNodes,
        cachedAt: Date.now()
      }
      
      const txn = db.transaction(['nodeChunks'], 'readwrite')
      const store = txn.objectStore('nodeChunks')
      
      await new Promise((resolve, reject) => {
        const req = store.put(chunk)
        req.onsuccess = () => resolve()
        req.onerror = () => reject(req.error)
      })
      
      nodeChunks = []
    }
    
    async function saveLinkChunk() {
      if (linkChunks.length === 0) return
      
      const chunkIndex = Math.floor((linksProcessed - linkChunks.length) / chunkSize)
      const chunk = {
        id: `${slug}_links_${chunkIndex}`,
        slug,
        chunkIndex,
        type: 'links',
        data: [...linkChunks],
        cachedAt: Date.now()
      }
      
      const txn = db.transaction(['linkChunks'], 'readwrite')
      const store = txn.objectStore('linkChunks')
      
      await new Promise((resolve, reject) => {
        const req = store.put(chunk)
        req.onsuccess = () => resolve()
        req.onerror = () => reject(req.error)
      })
      
      linkChunks = []
    }
    
    async function saveRemainingChunks() {
      if (nodeChunks.length > 0) await saveNodeChunk()
      if (linkChunks.length > 0) await saveLinkChunk()
    }
    
    async function saveMetadata() {
      // Try to extract metadata from buffer if we didn't get it during streaming
      if (!metadata || !qParams) {
        try {
          // Look for metadata and qParams in the buffer
          const metadataMatch = buffer.match(/"metadata"\s*:\s*({[^}]+})/)
          const qParamsMatch = buffer.match(/"qParams"\s*:\s*({[^}]+})/)
          
          if (metadataMatch && !metadata) {
            metadata = JSON.parse(metadataMatch[1])
          }
          if (qParamsMatch && !qParams) {
            qParams = JSON.parse(qParamsMatch[1])
          }
        } catch (e) {
          console.warn('Could not extract metadata from buffer')
          metadata = metadata || {}
          qParams = qParams || {}
        }
      }
      
      const metadataObj = {
        slug,
        metadata: metadata || {},
        qParams: qParams || {},
        totalNodes: nodesProcessed,
        totalLinks: linksProcessed,
        nodeChunks: Math.ceil(nodesProcessed / chunkSize),
        linkChunks: Math.ceil(linksProcessed / chunkSize),
        lastAccessed: Date.now(),
        version: 1
      }
      
      const txn = db.transaction(['graphMeta'], 'readwrite')
      const store = txn.objectStore('graphMeta')
      
      await new Promise((resolve, reject) => {
        const req = store.put(metadataObj)
        req.onsuccess = () => resolve()
        req.onerror = () => reject(req.error)
      })
    }
  }
  
  async function getFileProgressive(path, useCache = true, maxNodes = 5000) {
    // Cache storage 
    var __datacache = window.__datacache = window.__datacache || {}

    if (path.startsWith('./features/')) {
      path = path.replace('./features/', 'https://d1fk9w8oratjix.cloudfront.net/features/')
    }

    if (!window.isLocalServing){
      if (window.location.hostname === 'localhost' && path.startsWith('./data/')) {
        path = path.replace('./data/', 'https://d1fk9w8oratjix.cloudfront.net/data/')
      }

      if (window.location.hostname === 'localhost' && path.startsWith('./graph_data/')) {
        path = path.replace('./graph_data/', 'https://d1fk9w8oratjix.cloudfront.net/graph_data/')
      }
    }
    console.log('Getting path', path)
    
    // Return cached result if available 
    if (!useCache || !__datacache[path]) __datacache[path] = __fetch()
    return __datacache[path]

    async function __fetch() {
      var cacheOption = useCache ? 'force-cache' : 'no-cache'
      var res = await fetch(path, {cache: cacheOption})
      if (res.status == 500) {
        var resText = await res.text()
        console.log(resText, res) 
        throw '500 error'
      }

      var type = path.replaceAll('..', '').split('.').at(-1)
      if (type == 'csv') {
        return d3.csvParse(await res.text())
      } else if (type == 'npy') {
        return npyjs.parse(await res.arrayBuffer())
      } else if (type == 'json') {
        // Check Content-Length to determine if we should use streaming
        var transferEncoding = res.headers.get('Transfer-Encoding')

        if (transferEncoding == 'chunked') {
          console.log(`Streaming large JSON file`)
          return await streamParseJSON(res)
        } else {
          return await res.json()
        }
      } else if (type == 'jsonl') {
        var text = await res.text()
        return text.split(/\r?\n/).filter(d => d).map(line => JSON.parse(line))
      } else {
        return await res.text()
      }
    }
  }
  
  async function getFile(path, useCache = true) {
    // Enhanced version that supports progressive loading for graph data
    
    // Check if this is a graph data request and we should use progressive loading
    if (path.includes('./graph_data/') && path.endsWith('.json')) {
      const slug = path.split('/').pop().replace('.json', '')
      
      // Try to load from cache first
      const cachedData = await loadGraphDataProgressive(slug, 5000, 10000)
      if (cachedData) {
        console.log(`Loaded graph ${slug} from IndexedDB cache`)
        return cachedData
      }
      
      console.log(`Graph ${slug} not in cache, downloading and caching...`)
    }
    
    // Cache storage 
    var __datacache = window.__datacache = window.__datacache || {}

    if (path.startsWith('./features/')) {
      path = path.replace('./features/', 'https://d1fk9w8oratjix.cloudfront.net/features/')
    }

    if (!window.isLocalServing){
      if (window.location.hostname === 'localhost' && path.startsWith('./data/')) {
        path = path.replace('./data/', 'https://d1fk9w8oratjix.cloudfront.net/data/')
      }

      if (window.location.hostname === 'localhost' && path.startsWith('./graph_data/')) {
        path = path.replace('./graph_data/', 'https://d1fk9w8oratjix.cloudfront.net/graph_data/')
      }
    }
    
    // Return cached result if available 
    if (!useCache || !__datacache[path]) __datacache[path] = __fetch()
    return __datacache[path]

    async function __fetch() {
      var cacheOption = useCache ? 'force-cache' : 'no-cache'
      var res = await fetch(path, {cache: cacheOption})
      if (res.status == 500) {
        var resText = await res.text()
        console.log(resText, res) 
        throw '500 error'
      }

      var type = path.replaceAll('..', '').split('.').at(-1)
      if (type == 'csv') {
        return d3.csvParse(await res.text())
      } else if (type == 'npy') {
        return npyjs.parse(await res.arrayBuffer())
      } else if (type == 'json') {
        // Check Content-Length to determine if we should use streaming        
        var transferEncoding = res.headers.get('Transfer-Encoding')

        if (transferEncoding == 'chunked') {
          console.log(`Streaming large JSON file`)
          const data = await streamParseJSON(res)
          
          // If this is graph data, cache it for progressive loading
          if (path.includes('./graph_data/') && path.endsWith('.json')) {
            const slug = path.split('/').pop().replace('.json', '')
            await cacheGraphData(slug, data)
            
            // Return only the most important nodes initially
            const progressiveData = await loadGraphDataProgressive(slug, 5000, 10000)
            return progressiveData || data
          }
          
          return data
        } else {
          return await res.json()
        }
      } else if (type == 'jsonl') {
        var text = await res.text()
        return text.split(/\r?\n/).filter(d => d).map(line => JSON.parse(line))
      } else {
        return await res.text()
      }
    }
  }
  
  function addAxisLabel(c, xText, yText, title='', xOffset=0, yOffset=0, titleOffset=0){
    c.svg.select('.x').append('g')
      .translate([c.width/2, xOffset + 25])
      .append('text.axis-label')
      .text(xText)
      .at({textAnchor: 'middle', fill: '#000'})

    c.svg.select('.y')
      .append('g')
      .translate([yOffset -30, c.height/2])
      .append('text.axis-label')
      .text(yText)
      .at({textAnchor: 'middle', fill: '#000', transform: 'rotate(-90)'})

    c.svg
      .append('g.axis').at({fontFamily: 'sans-serif'})
      .translate([c.width/2, titleOffset -10])
      .append('text.axis-label.axis-title')
      .text(title)
      .at({textAnchor: 'middle', fill: '#000'})
  }

  function ggPlot(c){
    c.svg.append('rect.bg-rect')
      .at({width: c.width, height: c.height, fill: c.isBlack ? '#000' : '#EAECED'}).lower()
    c.svg.selectAll('.domain').remove()

    c.svg.selectAll('.x text').at({y: 4})
    c.svg.selectAll('.x .tick')
      .selectAppend('path').at({d: 'M 0 0 V -' + c.height, stroke: c.isBlack ? '#444' : '#fff', strokeWidth: 1})

    c.svg.selectAll('.y text').at({x: -3})
    c.svg.selectAll('.y .tick')
      .selectAppend('path').at({d: 'M 0 0 H ' + c.width, stroke: c.isBlack? '#444' : '#fff', strokeWidth: 1})

    ggPlotUpdate(c)
  }

  function ggPlotUpdate(c){
    c.svg.selectAll('.tick').selectAll('line').remove()

    c.svg.selectAll('.x text').at({y: 4})
    c.svg.selectAll('.x .tick')
      .selectAppend('path').at({d: 'M 0 0 V -' + c.height, stroke: c.isBlack ? '#444' : '#fff', strokeWidth: 1})

    c.svg.selectAll('.y text').at({x: -3})
    c.svg.selectAll('.y .tick')
      .selectAppend('path').at({d: 'M 0 0 H ' + c.width, stroke: c.isBlack? '#444' : '#fff', strokeWidth: 1})
  }

  function initRenderAll(fnLabels){
    var rv = {}
    fnLabels.forEach(label => {
      rv[label] = (ev) => Object.values(rv[label].fns).forEach(d => d(ev))
      rv[label].fns = []
    })

    return rv
  }
  
  function attachRenderAllHistory(renderAll, skipKeys=['hoverId', 'hoverIdx']) {
    // Add state pushing to each render function
    Object.keys(renderAll).forEach(key => {
      renderAll[key].fns.push(() => {
        if (skipKeys.includes(key)) return
        var simpleVisState = {...visState}
        skipKeys.forEach(key => delete simpleVisState[key])

        var url = new URL(window.location) 
        if (visState[key] == url.searchParams.get(key)) return
        url.searchParams.set(key, simpleVisState[key])
        history.pushState(simpleVisState, '', url)
      })
    })

    // Handle back/forward navigation
    d3.select(window).on('popstate.updateState', ev => {
      if (!ev.state) return
      ev.preventDefault()
      Object.keys(renderAll).forEach(key => {
        if (skipKeys.includes(key)) return 
        if (visState[key] == ev.state[key]) return
        visState[key] = ev.state[key]
        renderAll[key]()
      })
    })
  }
  
  function throttle(fn, delay){
    var lastCall = 0
    return (...args) => {
      if (Date.now() - lastCall < delay) return
      lastCall = Date.now()
      fn(...args)
    }
  }

  function debounce(fn, delay) {
    var timeout
    return (...args) => {
      clearTimeout(timeout)
      timeout = setTimeout(() => fn(...args), delay)
    }
  }

  function throttleDebounce(fn, delay) {
    var lastCall = 0
    var timeoutId

    return function (...args) {
      clearTimeout(timeoutId)
      var remainingDelay = delay - (Date.now() - lastCall)
      if (remainingDelay <= 0) {
        lastCall = Date.now()
        fn.apply(this, args)
      } else {
        timeoutId = setTimeout(() => {
          lastCall = Date.now()
          fn.apply(this, args)
        }, remainingDelay)
      }
    }
  }

  function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }
  
  function cache(fn){
    var cache = {}
    return function(...args){
      var key = JSON.stringify(args)
      if (!(key in cache)) cache[key] = fn.apply(this, args)
      return cache[key]
    }
  }
  var featureExamplesTooltipSel
  var featureExamples 
  var featureQueue = []
  function attachFeatureExamplesTooltip(sel, getFeatureParams, getNearby){
    if (!featureExamplesTooltipSel){
      featureExamplesTooltipSel = d3.select('body')
        .selectAppend('div.tooltip.feature-examples-tooltip.tooltip-hidden')
        .on('mouseover', mousemove)
        .on('mousemove', mousemove) 
        .on('mouseleave', mouseout)
      
      // Add touch event handler to body
      d3.select('body').on('click.feature-tooltip', ev => {
        // Don't trigger if touch is on tooltip or tooltipped element
        if (ev.target.closest('.feature-examples-tooltip') || ev.target.closest('.feature-examples-tooltipped')) return
        mouseout()
      })

      d3.select(window).on('scroll.feature-examples-tooltip', () => {
        if (featureExamplesTooltipSel.isFading || featureExamplesTooltipSel.isFaded) return
        mouseout()
      })

      featureExamplesTooltipSel.append('div.feature-nav')
      featureExamples = window.initFeatureExamples({
        containerSel: featureExamplesTooltipSel.append('div'),
        hideStaleOutputs: true,
      })

      if (window.__feature_tooltip_queue_timer) __feature_tooltip_queue_timer.stop()
      window.__feature_tooltip_queue_timer = d3.timer(() => {
        if (!featureQueue.length) return
        var feature = featureQueue.pop()
        featureExamples.loadFeature(feature.scan, feature.featureIndex)
      }, 250)
    }

    sel
      .on('mousemove.feature-examples-tooltip', mousemove)
      .on('mouseleave.feature-examples-tooltip', mouseout)
      .on('mouseenter.feature-examples-tooltip', function(ev, d){
        setTimeout(mousemove, 0)
        
        // skip moving if we're just bouncing in and out of the current feature
        if (featureExamplesTooltipSel.cur == d && !featureExamplesTooltipSel.classed('tooltip-hidden')) return 
        featureExamplesTooltipSel.cur = d
        
        featureExamplesTooltipSel.node().scrollTop = -200

        featureExamplesTooltipSel.isFaded = false
        featureExamplesTooltipSel.classed('tooltip-hidden', 0)

        // requires either featureIndex or featureIndices
        var {scan, featureIndex, featureIndices} = getFeatureParams(d)
        featureIndices = featureIndices ?? [featureIndex]
        featureIndex = featureIndex ?? featureIndices[0]

        var buttonSel = featureExamplesTooltipSel.select('.feature-nav').html('')
          .appendMany('div.button', featureIndices)
          .text((_, i) => 'Feature ' + (i + 1))
          .classed('active', idx => idx == featureIndex)
          .on('click', (ev, idx) => {
            featureExamples.renderFeature(scan, idx)
            buttonSel.classed('active', idx2 => idx2 == idx)
          })

        featureExamples.renderFeature(scan, featureIndex)

        d3.selectAll('.feature-examples-tooltipped').classed('feature-examples-tooltipped', 0)
        d3.select(this).classed('feature-examples-tooltipped', 1)

        var snBB = this.getBoundingClientRect()
        var ttBB = featureExamplesTooltipSel.node().getBoundingClientRect()
        var left = d3.clamp(20, (ev.clientX-ttBB.width/2), window.innerWidth - ttBB.width - 20)
        var top = snBB.top > innerHeight - snBB.bottom ?
            snBB.top - ttBB.height - 10 :
            snBB.bottom + 10
        featureExamplesTooltipSel.st({left, top, pointerEvents: 'all'})

        getNearby?.(d).forEach(e => featureQueue.push(e))
      })

    function mousemove(){
      if (window.__ttfade) window.__ttfade.stop()
      featureExamplesTooltipSel.isFading = false
      featureExamplesTooltipSel.isFaded = false
    }

    function mouseout(){
      if (featureExamplesTooltipSel.isFading) return

      if (window.__ttfade) window.__ttfade.stop()
      featureExamplesTooltipSel.isFading = true
      window.__ttfade = d3.timeout(() => {
        featureExamplesTooltipSel.classed('tooltip-hidden', 1).st({pointerEvents: 'none'})
        d3.selectAll('.feature-examples-tooltipped').classed('feature-examples-tooltipped', 0)
        featureExamplesTooltipSel.isFading = false
        featureExamplesTooltipSel.isFaded = true
      }, 250)
    }
  }
  
  async function initGraphSelect(sel, cgSlug){
    var {graphs} = await util.getFile('./data/graph-metadata.json')
    
    var selectSel = sel.html('').append('select.graph-prompt-select')
      .on('change', function() {
        cgSlug = this.value 
        // visState.clickedId = undefined
        util.params.set('slug', this.value)
        render()
      })
    
    var cgSel = sel.append('div.cg-container')
  
    selectSel.appendMany('option', graphs)
      .text(d => {
        var scanName = util.nameToPrettyPrint[d.scan] || d.scan
        var prefix = d.title_prefix ? d.title_prefix + ' ' : ''
        return prefix + scanName + ' — ' + d.prompt
      })
      .attr('value', d => d.slug)
      .property('selected', d => d.slug === cgSlug)
  
    function render() {
      initCg(cgSel.html(''), cgSlug, {
        isModal: true,
        // clickedId: visState.clickedId,
        // clickedIdCb: id => util.params.set('clickedId', id)
      })
      
      var m = graphs.find(g => g.slug == cgSlug)
      if (!m) return
      selectSel.at({title: m.prompt})
    }
    render()
  }
  
  function attachCgLinkEvents(sel, cgSlug, figmaSlug){
    sel
      .on('mouseover', () => util.getFile(`./graph_data/${cgSlug}.json`))
      .on('click', (ev) => {
        ev.preventDefault()
        
        if (window.innerWidth < 900 || window.innerHeight < 500) {
          return window.open(`./static_js/attribution_graphs/index.html?slug=${cgSlug}`, '_blank')
        }
  
        d3.select('body').classed('modal-open', true)
        var contentSel = d3.select('modal').classed('is-active', 1)
          .select('.modal-content').html('')
        
        util.initGraphSelect(contentSel, cgSlug)
        
        util.params.set('slug', cgSlug)
        if (figmaSlug) history.replaceState(null, '', '#' + figmaSlug)
      })
  }
  
  // TODO: tidy
  function ppToken(d){
    return d
  }
  
  function ppClerp(d){
    return d
  }
  
  
  var scanSlugToName = {
    'h35': 'jackl-circuits-runs-1-4-sofa-v3_0',
    '18l': 'jackl-circuits-runs-1-1-druid-cp_0',
    'moc': 'jackl-circuits-runs-12-19-valet-m_0'
  }
  
  var nameToPrettyPrint = {
    'jackl-circuits-runs-1-4-sofa-v3_0': 'Haiku',
    'jackl-circuits-runs-1-1-druid-cp_0': '18L',
    'jackl-circuits-runs-12-19-valet-m_0': 'Model Organism',
    'jackl-circuits-runs-1-12-rune-cp3_0': '18L PLTs',
  }

  
  // IndexedDB for large graph data caching
  async function openGraphDB() {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open('CircuitTracerGraphs', 1)
      
      request.onerror = () => reject(request.error)
      request.onsuccess = () => resolve(request.result)
      
      request.onupgradeneeded = (event) => {
        const db = event.target.result
        
        // Store for cached graph metadata
        if (!db.objectStoreNames.contains('graphMeta')) {
          const metaStore = db.createObjectStore('graphMeta', { keyPath: 'slug' })
          metaStore.createIndex('lastAccessed', 'lastAccessed')
        }
        
        // Store for node chunks
        if (!db.objectStoreNames.contains('nodeChunks')) {
          const nodeStore = db.createObjectStore('nodeChunks', { keyPath: 'id' })
          nodeStore.createIndex('slug', 'slug')
          nodeStore.createIndex('chunkIndex', 'chunkIndex')
        }
        
        // Store for link chunks  
        if (!db.objectStoreNames.contains('linkChunks')) {
          const linkStore = db.createObjectStore('linkChunks', { keyPath: 'id' })
          linkStore.createIndex('slug', 'slug')
          linkStore.createIndex('chunkIndex', 'chunkIndex')
        }
      }
    })
  }
  
  async function cacheGraphData(slug, data, chunkSize = 1000) {
    const db = await openGraphDB()
    
    console.log(`Caching graph data for ${slug} in IndexedDB...`)
    
    // Store metadata
    const metaTxn = db.transaction(['graphMeta'], 'readwrite')
    const metaStore = metaTxn.objectStore('graphMeta')
    
    const metadata = {
      slug,
      metadata: data.metadata,
      qParams: data.qParams,
      totalNodes: data.nodes?.length || 0,
      totalLinks: data.links?.length || 0,
      nodeChunks: Math.ceil((data.nodes?.length || 0) / chunkSize),
      linkChunks: Math.ceil((data.links?.length || 0) / chunkSize),
      lastAccessed: Date.now(),
      version: 1
    }
    
    await new Promise((resolve, reject) => {
      const req = metaStore.put(metadata)
      req.onsuccess = () => resolve()
      req.onerror = () => reject(req.error)
    })
    
    // Cache node chunks sorted by influence (most important first)
    if (data.nodes?.length) {
      const sortedNodes = [...data.nodes].sort((a, b) => (b.influence || 0) - (a.influence || 0))
      const nodeChunks = []
      
      for (let i = 0; i < sortedNodes.length; i += chunkSize) {
        nodeChunks.push({
          id: `${slug}_nodes_${Math.floor(i / chunkSize)}`,
          slug,
          chunkIndex: Math.floor(i / chunkSize),
          type: 'nodes',
          data: sortedNodes.slice(i, i + chunkSize),
          cachedAt: Date.now()
        })
      }
      
      const nodeTxn = db.transaction(['nodeChunks'], 'readwrite')
      const nodeStore = nodeTxn.objectStore('nodeChunks')
      
      for (const chunk of nodeChunks) {
        await new Promise((resolve, reject) => {
          const req = nodeStore.put(chunk)
          req.onsuccess = () => resolve()
          req.onerror = () => reject(req.error)
        })
      }
    }
    
    // Cache link chunks
    if (data.links?.length) {
      const linkChunks = []
      
      for (let i = 0; i < data.links.length; i += chunkSize) {
        linkChunks.push({
          id: `${slug}_links_${Math.floor(i / chunkSize)}`,
          slug,
          chunkIndex: Math.floor(i / chunkSize),
          type: 'links', 
          data: data.links.slice(i, i + chunkSize),
          cachedAt: Date.now()
        })
      }
      
      const linkTxn = db.transaction(['linkChunks'], 'readwrite')
      const linkStore = linkTxn.objectStore('linkChunks')
      
      for (const chunk of linkChunks) {
        await new Promise((resolve, reject) => {
          const req = linkStore.put(chunk)
          req.onsuccess = () => resolve()
          req.onerror = () => reject(req.error)
        })
      }
    }
    
    console.log(`Cached ${metadata.nodeChunks} node chunks and ${metadata.linkChunks} link chunks`)
    db.close()
  }
  
  async function loadGraphDataProgressive(slug, maxNodes = 5000, maxLinks = 10000) {
    const db = await openGraphDB()
    
    // Get metadata
    const metaTxn = db.transaction(['graphMeta'], 'readonly')
    const metaStore = metaTxn.objectStore('graphMeta')
    
    const metadata = await new Promise((resolve, reject) => {
      const req = metaStore.get(slug)
      req.onsuccess = () => resolve(req.result)
      req.onerror = () => reject(req.error)
    })
    
    if (!metadata) {
      db.close()
      return null // Not cached
    }
    
    console.log(`Loading ${maxNodes} most influential nodes from cache...`)
    
    // Load node chunks (most influential first)
    const nodeChunksNeeded = Math.ceil(maxNodes / 1000)
    const nodes = []
    
    const nodeTxn = db.transaction(['nodeChunks'], 'readonly')
    const nodeStore = nodeTxn.objectStore('nodeChunks')
    
    for (let i = 0; i < Math.min(nodeChunksNeeded, metadata.nodeChunks); i++) {
      const chunk = await new Promise((resolve, reject) => {
        const req = nodeStore.get(`${slug}_nodes_${i}`)
        req.onsuccess = () => resolve(req.result)
        req.onerror = () => reject(req.error)
      })
      
      if (chunk) {
        nodes.push(...chunk.data)
        if (nodes.length >= maxNodes) {
          nodes.splice(maxNodes) // Trim to exact size
          break
        }
      }
    }
    
    // Load corresponding links (only links between loaded nodes)
    const nodeIds = new Set(nodes.map(n => n.node_id))
    const links = []
    
    const linkTxn = db.transaction(['linkChunks'], 'readonly')
    const linkStore = linkTxn.objectStore('linkChunks')
    
    for (let i = 0; i < metadata.linkChunks && links.length < maxLinks; i++) {
      const chunk = await new Promise((resolve, reject) => {
        const req = linkStore.get(`${slug}_links_${i}`)
        req.onsuccess = () => resolve(req.result)
        req.onerror = () => reject(req.error)
      })
      
      if (chunk) {
        const relevantLinks = chunk.data.filter(link => 
          nodeIds.has(link.source) && nodeIds.has(link.target)
        )
        links.push(...relevantLinks)
        
        if (links.length >= maxLinks) {
          links.splice(maxLinks)
          break
        }
      }
    }
    
    db.close()
    
    const result = {
      metadata: metadata.metadata,
      qParams: metadata.qParams,
      nodes,
      links,
      _partial: true,
      _totalNodes: metadata.totalNodes,
      _totalLinks: metadata.totalLinks,
      _loadedNodes: nodes.length,
      _loadedLinks: links.length
    }
    
    console.log(`Loaded ${nodes.length}/${metadata.totalNodes} nodes and ${links.length}/${metadata.totalLinks} links from cache`)
    
    return result
  }
  
  async function loadMoreNodes(slug, currentNodeCount, additionalNodes = 2000) {
    const db = await openGraphDB()
    
    const startChunk = Math.floor(currentNodeCount / 1000)
    const endChunk = Math.floor((currentNodeCount + additionalNodes) / 1000)
    const nodes = []
    
    const nodeTxn = db.transaction(['nodeChunks'], 'readonly')
    const nodeStore = nodeTxn.objectStore('nodeChunks')
    
    for (let i = startChunk; i <= endChunk; i++) {
      const chunk = await new Promise((resolve, reject) => {
        const req = nodeStore.get(`${slug}_nodes_${i}`)
        req.onsuccess = () => resolve(req.result)
        req.onerror = () => reject(req.error)
      })
      
      if (chunk) {
        const startIdx = i === startChunk ? currentNodeCount % 1000 : 0
        const endIdx = i === endChunk ? (currentNodeCount + additionalNodes) % 1000 : chunk.data.length
        nodes.push(...chunk.data.slice(startIdx, endIdx))
      }
    }
    
    db.close()
    return nodes
  }
  
  return {
    scanSlugToName,
    nameToPrettyPrint,
    params,
    getFile,
    addAxisLabel,
    ggPlot,
    ggPlotUpdate,
    initRenderAll,
    attachRenderAllHistory,
    throttle,
    debounce,
    throttleDebounce,
    sleep,
    cache,
    initGraphSelect,
    attachCgLinkEvents,
    ppToken,
    ppClerp,
    attachFeatureExamplesTooltip,
    openGraphDB,
    cacheGraphData,
    loadGraphDataProgressive,
    loadMoreNodes,
    getFileProgressive,
  }
})()

window.init?.()
