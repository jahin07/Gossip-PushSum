defmodule Topology do
    def imp2Dloop(n, neighbor,l) do
        ran = :rand.uniform(n)
        if ran == l or Enum.member?(neighbor, ran) == true do
            imp2Dloop(n, neighbor, l)
        else
            ran
        end
    end
    def select_topology(topology, n, l) do
        max = n
        number2d = l
        cond do
            topology == "line" ->
                cond do
                    l == 1 -> neighbor = [l+1]
                    l == max -> neighbor = [l-1]
                    true -> neighbor = [l+1, l-1]       
                end

            topology == "full" -> neighbor=Enum.to_list(1..max)

            topology == "2D" or topology == "imp2D" ->
                j = :math.sqrt(n) |> round
                neighbor = []
                if rem(l,j) == 0 do
                    neighbor = neighbor ++ [l+1]
                end
                if rem(l+1,j) do
                    neighbor = neighbor ++ [l-1]
                end
                if l-j<0 do
                    neighbor = neighbor ++ [l+j]
                end
                if l - (n-j) >= 0 do
                    neighbor = neighbor ++ [l-j]
                end
                if n > 4 do
                    if rem(l,j) != 0 and rem(l+1,j) != 0 do
                        neighbor = neighbor ++ [l-1]
                        neighbor = neighbor ++ [l+1]
                    end
                    if l-j>0 and l - (n-j) <0 do
                        neighbor = neighbor ++ [l+j]
                        neighbor = neighbor ++ [l-j]
                    end
                    if l == j do
                        neighbor = neighbor ++ [l-j]
                        neighbor = neighbor ++ [l+j]
                    end
                end
                if topology == "imp2D" do
                    rnd = imp2Dloop(n, neighbor, l)
                    neighbor = neighbor ++ [rnd]
                end
                neighbor
            
            true -> "Select a valid topology"
        end
    end

    def checkRnd(topology, n, l) do
        nodeList = select_topology(topology, n, l)
        nodeList = Enum.filter(nodeList, fn(x) -> x != l == true end)
        nodeList = Enum.filter(nodeList, fn(x) -> x != 0 == true end)
        nodeList = Enum.filter(nodeList, fn(x) -> x <= n == true end)
        nodeList = Enum.uniq(nodeList)
        nodeList
    end
end

defmodule MasterNode do
  use GenServer

  # API 
  def add_blacklist(pid, message) do
    GenServer.cast(pid, {:add_blacklist, message})
  end

  def get_blacklist(pid) do
    GenServer.call(pid, :get_blacklist, :infinity)
  end

  def get_whitelist(pid, nodeId, topo, numNodes) do
    GenServer.call(pid, {:get_whitelist, nodeId, topo, numNodes}, :infinity)
  end

  def whiteRandom(topo, numNodes, nodeId, messages) do
    nodeList = Topology.checkRnd(topo, numNodes, nodeId)
    nodeList = Enum.filter(nodeList, fn el -> !Enum.member?(messages, el) end)
    nodeLen = Kernel.length(nodeList)
    topoCheck = false
    if topo == "line" or topo == "2D" do
      topoCheck = true
    end
    if nodeLen == 0 and topoCheck == true do
      :timer.sleep 1000
      Process.exit(:global.whereis_name(:"jahin"),:kill)
    end
    if nodeLen == 0 do
      whiteRandom(topo, numNodes, nodeId, messages)
    else
      randomNeighbor = :rand.uniform(nodeLen)
      Enum.at(nodeList, randomNeighbor-1)
    end
  end

  # SERVER

  def init(messages) do
    {:ok, messages}
  end

  def handle_call(:get_blacklist, _from, messages) do
    {:reply, messages, messages}
  end

  def handle_cast({:add_blacklist, new_message}, messages) do
    messages = [new_message | messages]
    messages = Enum.uniq(messages)
    {:noreply, messages}
  end

  def handle_call({:get_whitelist, nodeId, topo, numNodes}, _from, messages) do
    nodernd = whiteRandom(topo, numNodes, nodeId, messages)
    {:reply, nodernd, messages}
  end

end

defmodule Gossip do
  use GenServer

  # API

  def start_link do
    GenServer.start_link(__MODULE__, [])
  end

  def add_message(pid, message, number, topo, numNodes) do
    GenServer.cast(pid, {:add_message, message, number, topo, numNodes})
  end

  def s(n, b, topo, isTriggered, bonusparam) do
    blacklist = MasterNode.get_blacklist(:global.whereis_name(:"nodeMaster"))
    bllen = Kernel.length(blacklist)
    if topo == "line" or topo == "2D" do
      threshold = 0.1
    else 
      threshold = 0.5
    end
    # Randomly blacklist nodes
    connectionthreshold = threshold/2
    if bllen / n >= connectionthreshold and isTriggered == false do
      # randomly snap connection for specific percentage of nodes
      isTriggered = true
      Enum.map(1..bonusparam, fn(_) ->
         bonusnode = :rand.uniform(n)
         MasterNode.add_blacklist(:global.whereis_name(:"nodeMaster"), bonusnode)
      end)
    end
    if(bllen / n >= threshold) do
      IO.puts "Time = #{System.system_time(:millisecond) - b}"
      Process.exit(self(),:kill)
    end
    s(n, b, topo, isTriggered, bonusparam)
  end
  

  # SERVER

  def init(messages) do
    {:ok, messages}
  end

  def handle_cast({:add_message, new_message, number, topo, numNodes}, messages) do
    if messages == 9 do
      MasterNode.add_blacklist(:global.whereis_name(:"nodeMaster"), number)
    end
    r = MasterNode.get_whitelist(:global.whereis_name(:"nodeMaster"), number, topo, numNodes)
    nodeName = String.to_atom("node#{r}")
    :timer.sleep 1
    Gossip.add_message(:global.whereis_name(nodeName), new_message, r, topo, numNodes)
    {:noreply, messages+1}
  end

  def createNodes(times) do
    if times > 0 do
      nodeName = String.to_atom("node#{times}")
      {:ok, pid} = GenServer.start_link(Gossip, 1, name: nodeName)
      :global.register_name(nodeName,pid)
      createNodes(times-1)
    end
    
  end
end

defmodule PushSum do
  use GenServer
  # API

  def start_link do
    GenServer.start_link(__MODULE__, [])
  end

  def add_message(pid, message, number, topo, numNodes, halfS, halfW) do
      GenServer.cast(pid, {:add_message, message, number, topo, numNodes, halfS, halfW})
  end

  def s(n, b, topo, isTriggered, bonusparam) do
    blacklist = MasterNode.get_blacklist(:global.whereis_name(:"nodeMaster"))
    bllen = Kernel.length(blacklist)
    if topo == "line" or topo == "2D" do
      threshold = 0.1
    else 
      threshold = 0.5
    end
    connectionthreshold = threshold/2
    if bllen / n >= connectionthreshold and isTriggered == false do
      isTriggered = true
      Enum.map(1..bonusparam, fn(_) ->
         bonusnode = :rand.uniform(n)
         MasterNode.add_blacklist(:global.whereis_name(:"nodeMaster"), bonusnode)
      end)
    end

    if(bllen / n >= threshold) do
      IO.puts "Time = #{System.system_time(:millisecond) - b}"
      Process.exit(self(),:kill)
    end
    s(n, b, topo, isTriggered, bonusparam)
  end

  # SERVER

  def init(messages) do
      {:ok, messages}
  end


  def handle_cast({:add_message, new_message, number, topo, numNodes, halfS, halfW}, messages) do
    newS = Enum.at(messages,0) + halfS
    newW = Enum.at(messages,1) + halfW

    oldRatio = Enum.at(messages,0) / Enum.at(messages,1)
    newRatio = newS / newW

    oldCount = 0

    if oldRatio - newRatio < 0.0000000001 do
      if Enum.at(messages,2) == 2 do
        MasterNode.add_blacklist(:global.whereis_name(:"nodeMaster"), number)
      end
      oldCount = Enum.at(messages,2) + 1
    end

    halfS = newS / 2
    halfW = newW / 2

    newS = newS - halfS
    newW = newW - halfW

    newState = [newS, newW, oldCount]

    r = MasterNode.get_whitelist(:global.whereis_name(:"nodeMaster"), number, topo, numNodes)
    nodeName = String.to_atom("node#{r}")
    PushSum.add_message(:global.whereis_name(nodeName), new_message, r, topo, numNodes, halfS, halfW)
    {:noreply, newState}
  end

  def createNodes(times) do
    if times > 0 do
      nodeName = String.to_atom("node#{times}")
      {:ok, pid} = GenServer.start_link(PushSum, [times,1,0], name: nodeName)
      :global.register_name(nodeName,pid)
      createNodes(times-1)
    end
  end
end

defmodule Project2Bonus do
  def main(args) do
    b = System.system_time(:millisecond)
    :global.register_name(:"jahin",self())
    topo = Enum.at(args,1)
    numNodes = String.to_integer(Enum.at(args,0))
    algorithm = Enum.at(args,2)
    bonusparam = String.to_integer(Enum.at(args,3))
    if numNodes <= bonusparam do
      IO.puts "Failure nodes cannot be more than total nodes"
      Process.exit(:global.whereis_name(:"jahin"),:kill)
    end

    if topo == "2D" or topo == "imp2D" do
      sqrt = :math.sqrt(numNodes) |> Float.floor |> round
      numNodes = :math.pow(sqrt, 2) |> round
    end
    startingNode = :rand.uniform(numNodes)
    if algorithm == "gossip" do
      Gossip.createNodes(numNodes)
      {:ok, pid1} = GenServer.start_link(MasterNode, [], name: :"nodeMaster")
      :global.register_name(:"nodeMaster",pid1)
      :global.sync()
      Gossip.add_message(:global.whereis_name(:"node1"), "Gossip", startingNode, topo, numNodes)
      Gossip.s(numNodes, b, topo, false, bonusparam)
    end
    if algorithm == "push-sum" do
      PushSum.createNodes(numNodes)
      {:ok, pid1} = GenServer.start_link(MasterNode, [], name: :"nodeMaster")
      :global.register_name(:"nodeMaster",pid1)
      :global.sync()
      PushSum.add_message(:global.whereis_name(:"node1"), "Push-Sum", startingNode, topo, numNodes, 0, 0)
      PushSum.s(numNodes, b, topo, false, bonusparam)
    end
    
  end
end
