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
  
  async function getFile(path, useCache = true) {
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
        return await res.json()
      } else if (type == 'jsonl') {
        var text = await res.text()
        return text.split(/\r?\n/).filter(d => d).map(line => JSON.parse(line))
      } else {
        return await res.text()
      }
    }
  }
  
  // NEW: Chunked graph data loader that doesn't load everything into memory
  async function getGraphDataStreaming(path, onProgress) {
    // For backwards compatibility, if no chunked API available, fallback to old method
    if (!window.isLocalServing) {
      return getFile(path, false)
    }
    
    console.log('Loading graph metadata first:', path)
    
    // First, load only metadata to understand the graph size
    const metadataResponse = await fetch(`${path}?chunk=metadata`)
    const metadata = await metadataResponse.json()
    
    console.log(`Graph has ${metadata.node_count} nodes and ${metadata.link_count} links`)
    
    // Return a special object that implements chunked loading
    return new ChunkedGraphLoader(path, metadata)
  }
  
  // NEW: Chunked graph loader that fetches data on demand
  class ChunkedGraphLoader {
    constructor(basePath, metadata) {
      this.basePath = basePath
      this.metadata = metadata.metadata
      this.qParams = metadata.qParams
      this.nodeCount = metadata.node_count
      this.linkCount = metadata.link_count
      this._loadedNodes = new Map()
      this._loadedLinks = new Map()
      
      console.log(`ChunkedGraphLoader initialized for ${metadata.node_count} nodes, ${metadata.link_count} links`)
    }
    
    // Load initial chunk of nodes (top by influence)
    async loadInitialNodes(limit = 1000, sortBy = 'influence') {
      console.log(`Loading initial ${limit} nodes sorted by ${sortBy}`)
      
      const response = await fetch(`${this.basePath}?chunk=nodes&limit=${limit}&sort_by=${sortBy}&offset=0`)
      const data = await response.json()
      
      // Store loaded nodes
      data.nodes.forEach(node => this._loadedNodes.set(node.node_id, node))
      
      console.log(`Loaded ${data.nodes.length} initial nodes`)
      return data.nodes
    }
    
    // Load links for currently loaded nodes
    async loadLinksForNodes(nodeIds, limit = 2000) {
      console.log(`Loading links for ${nodeIds.length} nodes`)
      
      const nodeIdsParam = Array.from(nodeIds).join(',')
      const response = await fetch(`${this.basePath}?chunk=links&node_ids=${nodeIdsParam}&limit=${limit}`)
      const data = await response.json()
      
      // Store loaded links
      data.links.forEach(link => {
        const linkId = `${link.source}-${link.target}`
        this._loadedLinks.set(linkId, link)
      })
      
      console.log(`Loaded ${data.links.length} links`)
      return data.links
    }
    
    // Load neighborhood for a specific node
    async loadNodeNeighborhood(nodeId, maxLinks = 20) {
      console.log(`Loading neighborhood for node ${nodeId}`)
      
      const response = await fetch(`${this.basePath}?chunk=neighborhood&node_id=${nodeId}&max_links=${maxLinks}`)
      const data = await response.json()
      
      // Store new nodes and links
      data.nodes.forEach(node => this._loadedNodes.set(node.node_id, node))
      data.links.forEach(link => {
        const linkId = `${link.source}-${link.target}`
        this._loadedLinks.set(linkId, link)
      })
      
      console.log(`Loaded neighborhood: ${data.nodes.length} nodes, ${data.links.length} links`)
      
      return {
        nodes: Array.from(this._loadedNodes.values()),
        links: Array.from(this._loadedLinks.values()),
        metadata: this.metadata,
        qParams: this.qParams,
        _isOptimized: true,
        _originalCounts: { nodes: this.nodeCount, links: this.linkCount }
      }
    }
    
    // Get current loaded data
    getCurrentData() {
      return {
        nodes: Array.from(this._loadedNodes.values()),
        links: Array.from(this._loadedLinks.values()),
        metadata: this.metadata,
        qParams: this.qParams,
        _isOptimized: true,
        _originalCounts: { nodes: this.nodeCount, links: this.linkCount }
      }
    }
  }

  // NEW: Clear cached graph data to free memory
  function clearGraphCache(slug) {
    if (slug) {
      const cacheKey = `graph_${slug}`
      localStorage.removeItem(cacheKey)
      console.log('Cleared cache for:', slug)
    } else {
      // Clear all graph caches
      const keys = Object.keys(localStorage).filter(key => key.startsWith('graph_'))
      keys.forEach(key => localStorage.removeItem(key))
      console.log('Cleared all graph caches:', keys.length)
    }
  }

  // NEW: Chunked graph data manager
  class ChunkedGraphData {
    constructor(data) {
      this.metadata = data.metadata
      this.qParams = data.qParams
      this._allNodes = data.nodes
      this._allLinks = data.links
      this._loadedNodeIds = new Set()
      this._loadedNodes = []
      this._loadedLinks = []
      
      // Create indexes for efficient lookup
      this._nodeIndex = new Map(data.nodes.map(n => [n.node_id, n]))
      this._linksBySource = new Map()
      this._linksByTarget = new Map()
      
      // Build link indexes
      data.links.forEach(link => {
        if (!this._linksBySource.has(link.source)) this._linksBySource.set(link.source, [])
        if (!this._linksByTarget.has(link.target)) this._linksByTarget.set(link.target, [])
        this._linksBySource.get(link.source).push(link)
        this._linksByTarget.get(link.target).push(link)
      })
      
      console.log(`ChunkedGraphData initialized: ${data.nodes.length} nodes, ${data.links.length} links indexed`)
    }
    
    // Load initial chunk optimized for display
    loadInitialChunk(maxNodes = 1000, maxLinks = 2000, sortBy = 'influence', clickedNodeId = null) {
      console.log('Loading initial chunk...')
      
      // Sort all nodes by influence but only load top ones
      const sortedNodes = this._allNodes.slice().sort((a, b) => {
        const aVal = Math.abs(a[sortBy] || a.activation || 0)
        const bVal = Math.abs(b[sortBy] || b.activation || 0)
        return bVal - aVal
      })
      
      // Find critical nodes that must be included
      const criticalNodes = sortedNodes.filter(n => 
        n.feature_type === 'logit' || 
        n.feature_type === 'embedding' ||
        (clickedNodeId && n.jsNodeId === clickedNodeId)
      )
      
      // Get top nodes, ensuring critical nodes are included
      const remainingSlots = maxNodes - criticalNodes.length
      const otherTopNodes = sortedNodes
        .filter(n => !criticalNodes.includes(n))
        .slice(0, Math.max(0, remainingSlots))
      
      const selectedNodes = [...criticalNodes, ...otherTopNodes]
      this._loadChunk(selectedNodes, maxLinks)
      
      console.log(`Initial chunk loaded: ${this._loadedNodes.length} nodes, ${this._loadedLinks.length} links`)
      
      return {
        nodes: this._loadedNodes,
        links: this._loadedLinks,
        metadata: this.metadata,
        qParams: this.qParams,
        _isOptimized: true,
        _originalCounts: { nodes: this._allNodes.length, links: this._allLinks.length }
      }
    }
    
    // Load neighborhood around a specific node
    loadNodeNeighborhood(nodeId, maxLinksPerDirection = 20) {
      console.log(`Loading neighborhood for node ${nodeId}...`)
      
      const incomingLinks = (this._linksByTarget.get(nodeId) || [])
        .sort((a, b) => Math.abs(b.weight) - Math.abs(a.weight))
        .slice(0, maxLinksPerDirection)
      
      const outgoingLinks = (this._linksBySource.get(nodeId) || [])
        .sort((a, b) => Math.abs(b.weight) - Math.abs(a.weight))
        .slice(0, maxLinksPerDirection)
      
      // Collect all connected node IDs
      const connectedNodeIds = new Set([nodeId])
      ;[...incomingLinks, ...outgoingLinks].forEach(link => {
        connectedNodeIds.add(link.source)
        connectedNodeIds.add(link.target)
      })
      
      // Load nodes that aren't already loaded
      const newNodes = Array.from(connectedNodeIds)
        .filter(id => !this._loadedNodeIds.has(id))
        .map(id => this._nodeIndex.get(id))
        .filter(Boolean)
      
      // Add new nodes and links to loaded data
      this._loadedNodes.push(...newNodes)
      newNodes.forEach(n => this._loadedNodeIds.add(n.node_id))
      
      const newLinks = [...incomingLinks, ...outgoingLinks]
        .filter(link => 
          this._loadedNodeIds.has(link.source) && 
          this._loadedNodeIds.has(link.target) &&
          !this._loadedLinks.some(l => l.source === link.source && l.target === link.target)
        )
      
      this._loadedLinks.push(...newLinks)
      
      console.log(`Neighborhood loaded: +${newNodes.length} nodes, +${newLinks.length} links`)
      
      return {
        nodes: this._loadedNodes,
        links: this._loadedLinks,
        metadata: this.metadata,
        qParams: this.qParams,
        _isOptimized: true,
        _originalCounts: { nodes: this._allNodes.length, links: this._allLinks.length }
      }
    }
    
    // Internal method to load a chunk of nodes and their connecting links
    _loadChunk(nodes, maxLinks = 2000) {
      // Add nodes to loaded set
      nodes.forEach(n => {
        if (!this._loadedNodeIds.has(n.node_id)) {
          this._loadedNodes.push(n)
          this._loadedNodeIds.add(n.node_id)
        }
      })
      
      // Find links between loaded nodes
      const candidateLinks = []
      for (const nodeId of this._loadedNodeIds) {
        const outgoing = this._linksBySource.get(nodeId) || []
        candidateLinks.push(...outgoing.filter(link => this._loadedNodeIds.has(link.target)))
      }
      
      // Sort and limit links
      const sortedLinks = candidateLinks
        .sort((a, b) => Math.abs(b.weight) - Math.abs(a.weight))
        .slice(0, maxLinks)
      
      // Update loaded links (remove duplicates)
      const existingLinkIds = new Set(this._loadedLinks.map(l => `${l.source}-${l.target}`))
      const newLinks = sortedLinks.filter(l => !existingLinkIds.has(`${l.source}-${l.target}`))
      this._loadedLinks.push(...newLinks)
    }
    
    getCurrentData() {
      return {
        nodes: this._loadedNodes,
        links: this._loadedLinks,
        metadata: this.metadata,
        qParams: this.qParams,
        _isOptimized: true,
        _originalCounts: { nodes: this._allNodes.length, links: this._allLinks.length }
      }
    }
  }

  // NEW: Graph optimization utilities (now works with ChunkedGraphLoader)
  async function optimizeGraphData(data, options = {}) {
    // If data is a ChunkedGraphLoader, load initial data
    if (data instanceof ChunkedGraphLoader) {
      const nodes = await data.loadInitialNodes(options.maxNodes || 1000, options.sortBy || 'influence')
      const nodeIds = new Set(nodes.map(n => n.node_id))
      const links = await data.loadLinksForNodes(nodeIds, options.maxLinks || 2000)
      
      return data.getCurrentData()
    }
    
    // If data is already chunked, return as-is
    if (data instanceof ChunkedGraphData) {
      return data.getCurrentData()
    }
    
    // Fallback: Create chunked manager and load initial chunk
    const chunkedData = new ChunkedGraphData(data)
    return chunkedData.loadInitialChunk(
      options.maxNodes || 1000,
      options.maxLinks || 2000,
      options.sortBy || 'influence',
      options.clickedNodeId
    )
  }

  // NEW: Load top links for a specific node (now works with chunked data)
  async function loadNodeNeighborhood(originalData, nodeId, topLinksCount = 20) {
    // If originalData is a ChunkedGraphLoader instance, use its method
    if (originalData instanceof ChunkedGraphLoader) {
      return await originalData.loadNodeNeighborhood(nodeId, topLinksCount)
    }
    
    // If originalData is a ChunkedGraphData instance, use its method
    if (originalData instanceof ChunkedGraphData) {
      const result = originalData.loadNodeNeighborhood(nodeId, topLinksCount)
      return {
        nodes: result.nodes,
        links: result.links,
        centerNodeId: nodeId,
        totalAvailableLinks: result.links.length,
        incomingCount: result.links.filter(l => l.target === nodeId).length,
        outgoingCount: result.links.filter(l => l.source === nodeId).length
      }
    }
    
    // Fallback to original implementation for non-chunked data
    const { nodes, links } = originalData
    
    // Separate incoming and outgoing links
    const incomingLinks = links.filter(link => link.target === nodeId)
    const outgoingLinks = links.filter(link => link.source === nodeId)
    
    // Get top incoming links (sorted by absolute weight, descending)
    const topIncoming = incomingLinks
      .sort((a, b) => Math.abs(b.weight) - Math.abs(a.weight))
      .slice(0, topLinksCount) 
    
    // Get top outgoing links (sorted by absolute weight, descending)
    const topOutgoing = outgoingLinks
      .sort((a, b) => Math.abs(b.weight) - Math.abs(a.weight))
      .slice(0, topLinksCount)
    
    // Combine top incoming and outgoing links
    const topLinks = [...topIncoming, ...topOutgoing]
    
    // Collect all node IDs involved in these top links
    const nodeIds = new Set([nodeId]) // Always include the center node
    topLinks.forEach(link => {
      nodeIds.add(link.source)
      nodeIds.add(link.target)
    })
    
    // Get the actual node objects
    const neighborNodes = nodes.filter(n => nodeIds.has(n.node_id))
    
    console.log(`Loaded top ${topIncoming.length} incoming + ${topOutgoing.length} outgoing = ${topLinks.length} links for node ${nodeId} (${incomingLinks.length} incoming, ${outgoingLinks.length} outgoing available)`)

    return {
      nodes: neighborNodes,
      links: topLinks,
      centerNodeId: nodeId,
      totalAvailableLinks: incomingLinks.length + outgoingLinks.length,
      incomingCount: topIncoming.length,
      outgoingCount: topOutgoing.length
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

  
  return {
    scanSlugToName,
    nameToPrettyPrint,
    params,
    getFile,
    getGraphDataStreaming,
    clearGraphCache,
    ChunkedGraphData,
    ChunkedGraphLoader,
    optimizeGraphData,
    loadNodeNeighborhood,
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
  }
})()

window.init?.()
