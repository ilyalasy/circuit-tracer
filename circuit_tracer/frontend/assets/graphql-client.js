/**
 * GraphQL client for Circuit Tracer
 * Provides functions to query graph data without loading entire large JSON files
 */

window.GraphQLClient = (function() {
  const GRAPHQL_ENDPOINT = '/graphql';
  
  async function query(graphqlQuery, variables = {}) {
    const response = await fetch(GRAPHQL_ENDPOINT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: graphqlQuery,
        variables: variables
      })
    });
    
    if (!response.ok) {
      throw new Error(`GraphQL request failed: ${response.status} ${response.statusText}`);
    }
    
    const result = await response.json();
    
    if (result.errors) {
      throw new Error(`GraphQL errors: ${result.errors.map(e => e.message).join(', ')}`);
    }
    
    return result.data;
  }

  // Get metadata for all graphs
  async function getGraphMetadata() {
    const graphqlQuery = `
      query GetGraphMetadata {
        graphMetadata {
          slug
          scan
          transcoderList
          promptTokens
          prompt
          nodeThreshold
        }
      }
    `;
    
    const data = await query(graphqlQuery);
    return { graphs: data.graphMetadata };
  }

  // Get basic graph info without nodes/links
  async function getGraphBasicInfo(slug) {
    const graphqlQuery = `
      query GetGraphBasicInfo($slug: String!) {
        graphBasicInfo(slug: $slug) {
          metadata {
            slug
            scan
            transcoder_list: transcoderList
            prompt_tokens: promptTokens
            prompt
            node_threshold: nodeThreshold
          }
          qParams {
            pinned_ids: pinnedIds
            supernodes
            link_type: linkType
            clicked_id: clickedId
            sg_pos: sgPos
          }
        }
      }
    `;
    
    const data = await query(graphqlQuery, { slug });
    return data.graphBasicInfo;
  }

  // Get nodes with filtering and pagination
  async function getNodes(slug, options = {}) {
    const {
      offset = 0,
      limit = 1000,
      layerFilter = null,
      featureTypeFilter = null,
      ctxIdxFilter = null,
      influenceMin = null,
      sortByInfluence = true,
      maxNodes = 5000,
      nodeIds = null // Add nodeIds filter
    } = options;

    const graphqlQuery = `
      query GetNodes(
        $slug: String!,
        $offset: Int,
        $limit: Int,
        $layerFilter: String,
        $featureTypeFilter: String,
        $ctxIdxFilter: Int,
        $influenceMin: Float,
        $sortByInfluence: Boolean,
        $maxNodes: Int,
        $nodeIds: [String!]
      ) {
        nodes(
          slug: $slug,
          offset: $offset,
          limit: $limit,
          layerFilter: $layerFilter,
          featureTypeFilter: $featureTypeFilter,
          ctxIdxFilter: $ctxIdxFilter,
          influenceMin: $influenceMin,
          sortByInfluence: $sortByInfluence,
          maxNodes: $maxNodes,
          nodeIds: $nodeIds
        ) {
          nodes {
            node_id: nodeId
            feature
            layer
            ctx_idx: ctxIdx
            feature_type: featureType
            token_prob: tokenProb
            is_target_logit: isTargetLogit
            run_idx: runIdx
            reverse_ctx_idx: reverseCtxIdx
            jsNodeId: jsNodeId
            clerp
            influence
            activation
          }
          totalCount
          hasNextPage
        }
      }
    `;
    
    const data = await query(graphqlQuery, {
      slug,
      offset,
      limit,
      layerFilter,
      featureTypeFilter,
      ctxIdxFilter,
      influenceMin,
      sortByInfluence,
      maxNodes,
      nodeIds
    });
    
    return data.nodes;
  }

  // Get links with filtering and pagination
  async function getLinks(slug, options = {}) {
    const {
      offset = 0,
      limit = 1000,
      sourceFilter = null,
      targetFilter = null,
      weightMin = null,
      nodeIds = null // Add nodeIds filter
    } = options;

    const graphqlQuery = `
      query GetLinks(
        $slug: String!,
        $offset: Int,
        $limit: Int,
        $sourceFilter: String,
        $targetFilter: String,
        $weightMin: Float,
        $nodeIds: [String!]
      ) {
        links(
          slug: $slug,
          offset: $offset,
          limit: $limit,
          sourceFilter: $sourceFilter,
          targetFilter: $targetFilter,
          weightMin: $weightMin,
          nodeIds: $nodeIds
        ) {
          links {
            source
            target
            weight
          }
          totalCount
          hasNextPage
        }
      }
    `;
    
    const data = await query(graphqlQuery, {
      slug,
      offset,
      limit,
      sourceFilter,
      targetFilter,
      weightMin,
      nodeIds
    });
    
    return data.links;
  }

  // Get node connections (inputs and outputs) for a specific node
  async function getNodeConnections(slug, options = {}) {
    const {
      nodeId,
      maxInputs = 50,
      maxOutputs = 50,
      sortByWeight = true,
      weightMin = null
    } = options;

    const graphqlQuery = `
      query GetNodeConnections(
        $slug: String!,
        $nodeId: String!,
        $maxInputs: Int,
        $maxOutputs: Int,
        $sortByWeight: Boolean,
        $weightMin: Float
      ) {
        nodeConnections(
          slug: $slug,
          nodeId: $nodeId,
          maxInputs: $maxInputs,
          maxOutputs: $maxOutputs,
          sortByWeight: $sortByWeight,
          weightMin: $weightMin
        ) {
          inputs {
            sourceNodeId
            weight
          }
          outputs {
            targetNodeId
            weight
          }
        }
      }
    `;

    const data = await query(graphqlQuery, {
      slug,
      nodeId,
      maxInputs,
      maxOutputs,
      sortByWeight,
      weightMin
    });

    return data.nodeConnections;
  }

  // Get specific nodes by their IDs (now just calls getNodes with nodeIds filter)
  async function getNodesByIds(slug, options = {}) {
    const { nodeIds } = options;

    if (!nodeIds || nodeIds.length === 0) {
      return { nodes: [] };
    }

    // Just call the existing getNodes function with nodeIds filter
    const result = await getNodes(slug, {
      nodeIds: nodeIds,
      limit: nodeIds.length, // Set limit to the number of requested nodes
      sortByInfluence: false // Preserve order of requested IDs
    });

    return result;
  }

  // Load graph data incrementally (replaces the large JSON file loading)
  async function loadGraphData(slug, options = {}) {
    console.log(`Loading graph data for ${slug} using GraphQL`);
    
    // Get basic info first
    const basicInfo = await getGraphBasicInfo(slug);
    if (!basicInfo) {
      throw new Error(`Graph ${slug} not found`);
    }

    // Start loading nodes and links in parallel with some initial filtering
    const nodesPromise = getAllNodes(slug, options.pruningThreshold);
    const linksPromise = getAllLinks(slug, options.pruningThreshold);
    
    const [allNodes, allLinks] = await Promise.all([nodesPromise, linksPromise]);
    
    // Transform to match the original format
    const result = {
      metadata: {
        slug: basicInfo.metadata.slug,
        scan: basicInfo.metadata.scan,
        transcoder_list: basicInfo.metadata.transcoder_list || [],
        prompt_tokens: basicInfo.metadata.prompt_tokens || [],
        prompt: basicInfo.metadata.prompt,
        node_threshold: basicInfo.metadata.node_threshold
      },
      qParams: {
        pinned_ids: basicInfo.qParams.pinned_ids || [],
        supernodes: basicInfo.qParams.supernodes || [],
        link_type: basicInfo.qParams.link_type || "both",
        clicked_id: basicInfo.qParams.clicked_id || "",
        sg_pos: basicInfo.qParams.sgPos || ""
      },
      nodes: allNodes.map(n => ({
        node_id: n.node_id,
        feature: n.feature,
        layer: n.layer,
        ctx_idx: n.ctx_idx,
        feature_type: n.feature_type,
        token_prob: n.token_prob || 0.0,
        is_target_logit: n.is_target_logit || false,
        run_idx: n.run_idx || 0,
        reverse_ctx_idx: n.reverse_ctx_idx || 0,
        js_node_id: n.js_node_id,
        clerp: n.clerp || "",
        influence: n.influence,
        activation: n.activation
      })),
      links: allLinks.map(l => ({
        source: l.source,
        target: l.target,
        weight: l.weight
      }))
    };
    
    console.log(`Loaded ${result.nodes.length} nodes and ${result.links.length} links for ${slug}`);
    return result;
  }

  // Helper to get all nodes with pruning
  async function getAllNodes(slug, pruningThreshold = null) {
    const allNodes = [];
    let offset = 0;
    const limit = 1000; // Reasonable batch size
    let hasMore = true;
    
    // Use reasonable defaults for initial loading
    const maxNodes = 10000; // Don't load more than 10k nodes initially
    const sortByInfluence = true; // Always sort by influence for best nodes first
    
    while (hasMore && allNodes.length < maxNodes) {
      const result = await getNodes(slug, {
        offset,
        limit,
        influenceMin: pruningThreshold,
        sortByInfluence,
        maxNodes
      });
      
      allNodes.push(...result.nodes);
      hasMore = result.hasNextPage && allNodes.length < maxNodes;
      offset += limit;
      
      // Show progress and break early if we have enough high-influence nodes
      if (allNodes.length >= 5000) {
        console.log(`Loaded ${allNodes.length} high-influence nodes - stopping for performance`);
        break;
      }
      
      if (offset % 5000 === 0) {
        console.log(`Loaded ${allNodes.length} nodes...`);
      }
    }
    
    console.log(`Finished loading ${allNodes.length} nodes (sorted by influence)`);
    return allNodes;
  }

  // Helper to get all links with pruning
  async function getAllLinks(slug, pruningThreshold = null) {
    const allLinks = [];
    let offset = 0;
    const limit = 2000; // Larger batches for efficiency
    let hasMore = true;
    
    while (hasMore) {
      const result = await getLinks(slug, {
        offset,
        limit,
        weightMin: pruningThreshold // Use pruning threshold as weight filter
      });
      
      allLinks.push(...result.links);
      hasMore = result.hasNextPage;
      offset += limit;
      
      // Show progress for large datasets
      if (offset % 10000 === 0) {
        console.log(`Loaded ${allLinks.length} links...`);
      }
    }
    
    return allLinks;
  }

  // Save graph parameters using GraphQL mutation
  async function saveGraphParams(slug, qParams) {
    const graphqlQuery = `
      mutation SaveGraphParams($slug: String!, $qParams: String!) {
        saveGraphParams(slug: $slug, qParams: $qParams)
      }
    `;
    
    const data = await query(graphqlQuery, {
      slug,
      qParams: JSON.stringify(qParams)
    });
    
    return data.saveGraphParams;
  }

  return {
    getGraphMetadata,
    getGraphBasicInfo,
    getNodes,
    getLinks,
    getNodeConnections,
    getNodesByIds,
    loadGraphData,
    saveGraphParams,
    query // Export raw query function for custom queries
  };
})();

window.init?.();
