import akka.actor._
import akka.routing.RoundRobinRouter
import scala.util.Random
import scala.concurrent.duration._

object Project2 {
  
  def main(args: Array[String]){
    
    var numOfNodes =1000
    var topology :String = "full"		//full;2D;line;imp2D
    var algorithm :String = "push-sum"		//gossip;push-sum
    
    //	If topology is "2D", round up numOfNodes if needed
    if(topology.equals("2D") || topology.equals("imp2D")){
        var root :Double = java.lang.Math.sqrt(numOfNodes.toDouble)
        var ceilingRoot :Int = root.toInt
        
        if(root > ceilingRoot.toDouble){
          ceilingRoot += 1
        }
        numOfNodes = ceilingRoot * ceilingRoot
    }
    
    start(numOfNodes, topology, algorithm)
  }
  
  case class Start
  case class Register(i :Int)
  case class GiveIndex(i :Int)
  case class FinishBuildTopology
  case class FinishGossip(index :Int, flag: Int, maxNumOfRoumors :Int)
  case class BuildTopology
  case class GiveNeighbour(listOfNeighbour :List[ActorRef], listOfNeighbourIndex :List[Int])
//  case class GiveNeighbourImp2D(listOfNeighbour :List[ActorRef], listOfNeighbourIndex :List[Int], listOfRandomNeighbour :List[ActorRef], listofRandomNeighbourIndex :List[Int])
  case class GossipInitialization
  case class GossipPassRoumors(bossRef :ActorRef)
  case class GossipReceiveRoumors(bossRef :ActorRef)
  case class RemoveNeighbour(removeIndex :Int)
  case class PushSum
  case class PushSumReceive(sValue :Double, wValue :Double, bossRef :ActorRef)
  case class PushSumSend(bossRef :ActorRef)
  case class FinishPushSum(index :Int, sw :Double, flag :Int)
  
  case class Report
  case class PushSumPass(sValue :Double, wValue :Double, bossRef :ActorRef, flag :Int)
  case class SendBackSW(sValue :Double, wValue :Double, bossRef :ActorRef)
  case class FinishPushSum1(index :Int, sw :Double, flag :Int)
  case class PushSumRemoveNeighbour(index :Int, sValue :Double, wValue :Double, bossRef :ActorRef)
  
  case class SendMSG(sValue :Double, wValue :Double, bossRef :ActorRef)
  case class ReNeighbour(reIndex :Int ,bossRef :ActorRef)
  case class ReSend(bossRef :ActorRef)
  
  class Master(numOfNodes :Int, topology :String, algorithm :String) extends Actor{
    
    var actorList = List[ActorRef]()
    var actorIndex = List[Int]()
    var numOfRegister :Int = 0
    var numOfFinish :Int = 0
    var startTime :Long = _
    var FinishRecord = new Array[Int](numOfNodes + 1)
    var numOfExecutedNodes :Int = 0
    var numOfDeadNodes :Int = 0
    
    val nodesRouter = context.actorOf(Props[Nodes].withRouter(RoundRobinRouter(numOfNodes)), name = "nodesRouter")
    
    def BuildTopology(numOfNodes :Int, topology :String) {
      
      if(topology.equals("full")){
        for(i <- 0 to (numOfNodes - 1)){
          var index :Int = actorIndex(i)
          var tRef = List[ActorRef]()
          var tIndex = List[Int]()
//          println(index)
//          println(actorIndex)
          for(i <- 0 to (actorIndex.length - 1)){
            if(actorIndex(i) != index){
              tRef = actorList(i)::tRef
              tIndex = actorIndex(i)::tIndex
            }
          }
//          println(tIndex)
          actorList(i) ! GiveNeighbour(tRef, tIndex)
        }
      }
      else if(topology.equals("2D")){
        var ceilingRoot :Int = java.lang.Math.sqrt(numOfNodes.toDouble).toInt
        for(i <- 0 to (numOfNodes - 1)){
          var temActorList = List[ActorRef]()
          var temActorIndex = List[Int]()
           
          if(actorIndex(i) % ceilingRoot != 1 && actorIndex(i) - 1 > 0){	//add left neighbor to list
            temActorList = actorList(actorIndex.indexOf(actorIndex(i) - 1))::temActorList
            temActorIndex = actorIndex(actorIndex.indexOf(actorIndex(i) - 1))::temActorIndex
          }
          if(actorIndex(i) % ceilingRoot != 0 && actorIndex(i) + 1 <= numOfNodes){	//add right neighbor to list
            temActorList = actorList(actorIndex.indexOf(actorIndex(i) + 1))::temActorList
            temActorIndex = actorIndex(actorIndex.indexOf(actorIndex(i) + 1))::temActorIndex
          }
          if(actorIndex(i) - ceilingRoot > 0){	//add below neighbor to list
            temActorList = actorList(actorIndex.indexOf(actorIndex(i) - ceilingRoot))::temActorList
            temActorIndex = actorIndex(actorIndex.indexOf(actorIndex(i) - ceilingRoot))::temActorIndex
          }
          if(actorIndex(i) + ceilingRoot <= numOfNodes){	//add above neighbor to list
            temActorList = actorList(actorIndex.indexOf(actorIndex(i) + ceilingRoot))::temActorList
            temActorIndex = actorIndex(actorIndex.indexOf(actorIndex(i) + ceilingRoot))::temActorIndex
          }
          
          actorList(i) ! GiveNeighbour(temActorList, temActorIndex)
        }
          
      }
      else if(topology.equals("line")){
        for(i <- 0 to (numOfNodes - 1)){
          var temActorList = List[ActorRef]()
          var temActorIndex = List[Int]()
          
          if(actorIndex(i) == 1){
            temActorList = actorList(actorIndex.indexOf(2))::temActorList
            temActorIndex = actorIndex(actorIndex.indexOf(2))::temActorIndex
          } 
          else if(actorIndex(i) == numOfNodes){
            temActorList = actorList(actorIndex.indexOf(numOfNodes - 1))::temActorList
            temActorIndex = actorIndex(actorIndex.indexOf(numOfNodes - 1))::temActorIndex
          }
          else{
            temActorList = actorList(actorIndex.indexOf(actorIndex(i) - 1))::temActorList
            temActorList = actorList(actorIndex.indexOf(actorIndex(i) + 1))::temActorList
            temActorIndex = actorIndex(actorIndex.indexOf(actorIndex(i) - 1))::temActorIndex
            temActorIndex = actorIndex(actorIndex.indexOf(actorIndex(i) + 1))::temActorIndex
          }
          
          actorList(i) ! GiveNeighbour(temActorList, temActorIndex)
        }
      }
      else if(topology.equals("imp2D")){
        var ceilingRoot :Int = java.lang.Math.sqrt(numOfNodes.toDouble).toInt
        for(i <- 0 to (numOfNodes - 1)){
          var temActorList = List[ActorRef]()
          var temActorIndex = List[Int]()
          var removeIndex = List[Int]()
          
	 //     removeIndex = i::removeIndex
          removeIndex = actorIndex(i)::removeIndex
          
          if(actorIndex(i) % ceilingRoot != 1 && actorIndex(i) - 1 > 0){	//add left neighbor to list
            temActorList = actorList(actorIndex.indexOf(actorIndex(i) - 1))::temActorList
            temActorIndex = actorIndex(actorIndex.indexOf(actorIndex(i) - 1))::temActorIndex
            
//            removeIndex = actorIndex.indexOf(actorIndex(i) - 1)::removeIndex
            removeIndex = (actorIndex(i) - 1)::removeIndex
          }
          if(actorIndex(i) % ceilingRoot != 0 && actorIndex(i) + 1 <= numOfNodes){	//add right neighbor to list
            temActorList = actorList(actorIndex.indexOf(actorIndex(i) + 1))::temActorList
            temActorIndex = actorIndex(actorIndex.indexOf(actorIndex(i) + 1))::temActorIndex
            
//            removeIndex = actorIndex.indexOf(actorIndex(i) + 1)::removeIndex
            removeIndex = (actorIndex(i) + 1)::removeIndex
          }
          if(actorIndex(i) - ceilingRoot > 0){	//add below neighbor to list
            temActorList = actorList(actorIndex.indexOf(actorIndex(i) - ceilingRoot))::temActorList
            temActorIndex = actorIndex(actorIndex.indexOf(actorIndex(i) - ceilingRoot))::temActorIndex
            
//            removeIndex = actorIndex.indexOf(actorIndex(i) - ceilingRoot)::removeIndex
            removeIndex = (actorIndex(i) - ceilingRoot)::removeIndex
          }
          if(actorIndex(i) + ceilingRoot <= numOfNodes){	//add above neighbor to list
            temActorList = actorList(actorIndex.indexOf(actorIndex(i) + ceilingRoot))::temActorList
            temActorIndex = actorIndex(actorIndex.indexOf(actorIndex(i) + ceilingRoot))::temActorIndex
            
//            removeIndex = actorIndex.indexOf(actorIndex(i) + ceilingRoot)::removeIndex
            removeIndex = (actorIndex(i) + ceilingRoot)::removeIndex
          }
          
          //////	 add a random neighbour
          var flag :Boolean = true
          while(flag){
            var t :Int = (numOfNodes * java.lang.Math.random()).toInt + 1
            if(removeIndex.exists(s => s == t)){
              flag = true
            }
            else{	//success to get random neighbour
//              temActorList = actorList(t)::temActorList
//              temActorIndex = actorIndex(t)::temActorIndex
              temActorList = actorList(actorIndex.indexOf(t))::temActorList
              temActorIndex = t::temActorIndex
              flag = false
            }
          }
          
          actorList(i) ! GiveNeighbour(temActorList, temActorIndex)
        }
      }
      else{
        println("Topology Type is not acceptable!")
        context.system.shutdown()
      }
    }
    
    def runAlgorithm(algorithm :String){
      
      ////////////////   
      for(i <- 1 to FinishRecord.length - 1){
        FinishRecord(i) = 0
      }
      /////////////////////
      numOfFinish = 0
      
      if(algorithm.equals("gossip")){
        var i :Int = (actorList.length * java.lang.Math.random()).toInt
  //      println("The random num in 'runAlgorithm' is " + i + " The index is: " + actorIndex(i))
        actorList(i) ! GossipInitialization
      }
      else if(algorithm.equals("push-sum")){
        var i :Int = (actorList.length * java.lang.Math.random()).toInt
        actorList(i) ! PushSum
      }
      else{
        println("Algorithm Type is not acceptable!")
        context.system.shutdown()
      }
    }
    
    def receive = {
      
      case Start =>
        for(i <- 1 to numOfNodes)
          nodesRouter ! GiveIndex(i)
      
      case Register(i) =>
        actorList = sender::actorList
        actorIndex = i::actorIndex
        
//        println("The index of the node is: " + i + ". The ref is: " + sender)
        
        numOfRegister += 1
        //finish building topology
        if(numOfRegister == numOfNodes){
          numOfFinish = 0
          BuildTopology(numOfNodes, topology)
        }
        
      case FinishBuildTopology =>
        numOfFinish += 1
        if(numOfFinish == numOfRegister){
          println("Finish building topology!")
          startTime = System.currentTimeMillis()
          runAlgorithm(algorithm)
        }
        
      case FinishGossip(index, flag, maxNumOfRoumors) =>
//        numOfFinish += 1
        var j :Int = 0
        
        var t = FinishRecord(index)
        if(FinishRecord(index) == 0){
          FinishRecord(index) = 1
          numOfFinish += 1
          if(flag == maxNumOfRoumors){
            numOfExecutedNodes += 1
          }
          if(flag < maxNumOfRoumors){
            numOfDeadNodes += 1
          }
        }
        else{
            println("##########The Dead Note " + index +" Record Twice!! FFFFFFFFFFFFFFucking!!!!!!!!!!" + "num of roumors: " + flag)
        }
        
        if(numOfFinish > numOfNodes){
          println(numOfFinish + " OverFlow!!!!! " + "The nodes " + index + " with # of messages " + flag + " RecordBit: " + FinishRecord(index))
        }
        
        println("The privious RecordBit is " + t + " @@@@@@@@@@@@ # of finish messages is: " + numOfFinish + " From index " + index + " with the # of roumors " + flag + " RecordBit " + FinishRecord(index))
        if(numOfFinish == numOfRegister){
          println("There are " + numOfExecutedNodes +" nodes executed successfully! And there are " + numOfDeadNodes +" dead!")
          println("Finish gossip!")
          println((System.currentTimeMillis() - startTime).millis)
          context.system.shutdown()
        }
        
      case FinishPushSum(index, sw, flag) =>
        var t :Int = FinishRecord(index)
        
        if(FinishRecord(index) == 0){
          FinishRecord(index) = 1
          numOfFinish += 1
          if(flag == 1){
            numOfExecutedNodes += 1
          }
          if(flag == -1){
            numOfDeadNodes += 1
          }
        }
        else{
            println("##########The Dead Note " + index +" Record Twice!! FFFFFFFFFFFFFFucking!!!!!!!!!!" + "with the flag " + flag)
        }
        
        if(numOfFinish > numOfNodes){
          println(numOfFinish + " OverFlow!!!!! " + "The nodes " + index + " with # of messages " + sw + " RecordBit: " + FinishRecord(index))
        }
        
        println("The privious RecordBit is " + t + " @@@@@@@@@@@@ # of finish messages is: " + numOfFinish + " From index " + index + " with the # of s/w value " + sw + " with flag " + flag)
        if(numOfFinish == numOfRegister){
          println("There are " + numOfExecutedNodes +" nodes executed successfully! And there are " + numOfDeadNodes +" dead!")
          println("Finish Push-Sum!")
          println((System.currentTimeMillis() - startTime).millis)
          context.system.shutdown()
        }
        
      case FinishPushSum1(index, sw, flag) =>
        
        if(FinishRecord(index) == 0){
          FinishRecord(index) = 1
          numOfFinish += 1
          println("The node " + index + " converges with s/w value " + sw)
//          for(i <- 0 to (actorList.length - 1))
//            actorList(i) ! Report
        }
        
//        if(numOfFinish == numOfRegister){
        if(flag == 1){
          println("The node " + index + " is dead!")
          for(i <- 0 to (actorList.length - 1))
            actorList(i) ! Report
          println("Finish Push-Sum!")
          println((System.currentTimeMillis() - startTime).millis)
          context.system.shutdown()
        }
        
      case _ =>
        println("Unknown Message to Boss!")
    }
    
  }
  
  class Nodes extends Actor{
    
    var index :Int = 0
    var myNeighbourRef = List[ActorRef]()
    var myNeighbourIndex = List[Int]()
    var numOfNeighbours :Int = 0
    var deadNeighbourList = List[Int]()
    
    val maxNumOfRoumors :Int = 10
    var numOfRoumors :Int = 0
    
    var s :Double = 0
    var w :Double = 1
    var round1 :Double = 0.0
    var round2 :Double = 0.0
    var round3 :Double = 0.0
    
    var stop :Boolean = false;
    
    def ranNeighbourIndex() :Int = {
        var i :Int = (numOfNeighbours * java.lang.Math.random()).toInt
        i
    }
    
    def assignRoundValue() = {	//should update s,w value first, then call this function
      if(round1 == 0.0){
        round1 = s / w
      }
      else if(round2 == 0.0){
        round2 = round1
        round1 = s / w
      }
      else if(round3 == 0.0){
        round3 = round2
        round2 = round1
        round1 = s / w
      }
      else{
        round3 = round2
        round2 = round1
        round1 = s / w
      }
    }
    
    def isClosed(d1 :Double, d2 :Double) :Boolean = {
      if(d1 < d2){
          if(d2 < d1 + 0.000000001)
            true
          else
            false
        }
        else{
          if(d1 < d2 + 0.000000001)
            true
          else
            false
        }
    }
    
    def isTerminated() :Boolean = {	//should first call this function, then call the function "assignRoundValue"
      var curValue :Double = s / w
      if(round1 == 0.0 || round2 == 0.0 || round3 == 0.0){
        false
      }
      else{
        if(isClosed(curValue,round1) && isClosed(curValue, round2) && isClosed(curValue, round3) && isClosed(round1, round2) && isClosed(round1, round3) && isClosed(round3, round2)){
          true
        }
        else
          false
      }
    }
    
    def receive = {
      case GiveIndex(i) =>
        index = i
        s = i
        stop = false
      	sender ! Register(i)
      	
      case GiveNeighbour(listOfNeighbour, listOfNeighbourIndex) =>
//        var tRef = List[ActorRef]()
//        var tIndex = List[Int]()
//        for(i <- 0 to (listOfNeighbourIndex.length - 1)){
//          if(index != listOfNeighbourIndex(i)){
//            tIndex = listOfNeighbourIndex(i)::tIndex
//            tRef = listOfNeighbour(i)::tRef
//          }
//        }
//        myNeighbourRef = tRef
//        myNeighbourIndex = tIndex
//        numOfNeighbours = myNeighbourIndex.length
        myNeighbourRef = listOfNeighbour:::myNeighbourRef
        myNeighbourIndex = listOfNeighbourIndex:::myNeighbourIndex
        numOfNeighbours += myNeighbourIndex.length
//        println(index)
//        println(myNeighbourIndex)
//        println("The index of this node is: " + index  + ". My neighbour indexes are: " + myNeighbourIndex + ". My neighbour Ref is: " + myNeighbourRef + ". The number of neighbours is: " + numOfNeighbours)
        sender ! FinishBuildTopology
        
      case GossipInitialization =>
        println("The initialized the node index: " + index)
        numOfRoumors += 1
        if(numOfRoumors < maxNumOfRoumors){
          var i :Int = ranNeighbourIndex()
          myNeighbourRef(i) ! GossipReceiveRoumors(sender)
        }
          
      case GossipPassRoumors(bossRef) =>	//say, the sender group
        if(deadNeighbourList.length == numOfNeighbours && numOfRoumors != maxNumOfRoumors){
          deadNeighbourList = -1::deadNeighbourList
          if(index == 49 || index == 40 || index == 60){
            println("This node " + index + " is dead, cannot execute!")
          }
          println("This node " + index + " is dead, cannot execute!")
       //   context.stop(self)
          bossRef ! FinishGossip(index, flag = numOfRoumors, maxNumOfRoumors)
        }
    //      println("This node " + index + " is dead, cannot execute!")
        if(numOfRoumors < maxNumOfRoumors  && deadNeighbourList.length < numOfNeighbours){	//this node must be infected before
     //     numOfRoumors += 1
        	var i :Int = ranNeighbourIndex()
        	myNeighbourRef(i) ! GossipReceiveRoumors(bossRef)
        	
        	if(index == 50){
        	  println("The index " + index + " send messages to node " + myNeighbourIndex(i))
        	  println("bThe actor index : " + index + ". The # of passing roumors is: " + numOfRoumors)
        	}
          
          println("bThe actor index : " + index + ". The # of passing roumors is: " + numOfRoumors)
          
        }
        if(numOfRoumors == maxNumOfRoumors){
          if(index == 40 || index == 60 || index == 49){
            println("!!!!!!!!!!!!!!!!!!!!The index: " + index + ". The time  passing roumors is " + maxNumOfRoumors)
          }
          
//          println("!!!!!!!!!!!!!!!!!!!!The index: " + index + ". The time  passing roumors is " + maxNumOfRoumors)
          bossRef ! FinishGossip(index, flag = numOfRoumors, maxNumOfRoumors)
        }
        
      case GossipReceiveRoumors(bossRef) =>
        if(numOfRoumors == 0){	//this is a susceptible node, need to add PassRoumor Group
          numOfRoumors += 1
          var i :Int = ranNeighbourIndex()
          myNeighbourRef(i) ! GossipReceiveRoumors(bossRef)
          sender ! GossipPassRoumors(bossRef)
          
//          println("cThe actor index : " + index + ". The # of passing roumors is: " + numOfRoumors)
        }
        ///////////////////////Old version
//        else if(numOfRoumors < maxNumOfRoumors){	//this is an infected node, no need to add PassRoumor Group
//          numOfRoumors += 1
//          sender ! GossipPassRoumors(bossRef)
//          
//          println("dThe actor index : " + index + ". The # of passing roumors is: " + numOfRoumors)
//        }
//        else{	//receive "maxNumOfRoumors" rumors
//          //sender message to its neighbor to update their neighbor lists
//          sender ! RemoveNeighbour(index)
//          
//          sender ! GossipPassRoumors(bossRef)
//        }
        else if(numOfRoumors == maxNumOfRoumors){	//receive "maxNumOfRoumors" rumors
          //sender message to its neighbor to update their neighbor lists
          sender ! RemoveNeighbour(index)
          
          sender ! GossipPassRoumors(bossRef)
        }
        else if(numOfRoumors < maxNumOfRoumors){	//this is an infected node, no need to add PassRoumor Group
          numOfRoumors += 1
          sender ! GossipPassRoumors(bossRef)
          
//          println("dThe actor index : " + index + ". The # of passing roumors is: " + numOfRoumors)
        }
        else{}
        
        
      case RemoveNeighbour(removeIndex) =>
        if(!deadNeighbourList.exists(s => s==removeIndex)){
          deadNeighbourList = removeIndex::deadNeighbourList
          
          if(removeIndex == 49 || removeIndex == 40 || removeIndex == 60){
            println("The node " + removeIndex + " is removed from the node " + index + " neighbour list!")
          }
          
        }
        
      case PushSum =>
        println("The initialized the node index: " + index)
        
        var i :Int = ranNeighbourIndex()
        assignRoundValue()
        if(!isTerminated()){
          var sValue :Double = s
          var wValue :Double = w
          s /= 2
          w /= 2
//          myNeighbourRef(i) ! PushSumReceive(sValue / 2, wValue / 2, sender)
          myNeighbourRef(i) ! PushSumPass(sValue, wValue, sender, flag = 0)
//          myNeighbourRef(i) ! SendMSG(sValue/2, wValue/2, sender)
        }
        
      case PushSumReceive(sValue, wValue, bossRef) =>
//        if(isTerminated()){
//          sender ! RemoveNeighbour(index)
//          sender ! PushSumSend(bossRef)
//        }
//        else{
//          if(round1 == 0.0){	//this is a susceptible node, need to add PassRoumor Group
//            s += sValue
//            w += wValue
//            assignRoundValue()
//            println("The index " + index + " the s/w value change to " + (s / w))
//            var i = ranNeighbourIndex()
//            var sVal :Double = s
//            var wVal :Double = w
//            s /= 2
//            w /= 2
//            myNeighbourRef(i) ! PushSumReceive(sVal / 2, wVal / 2, bossRef)
//            sender ! PushSumSend(bossRef)
//          }
//          else{
//            s += sValue
//            w += wValue
//            if(isTerminated()){
//              sender ! RemoveNeighbour(index)
//              sender ! PushSumSend(bossRef)
//            }
//            else{
//              assignRoundValue()
//              sender ! PushSumSend(bossRef)
//            }
//          }
//        }
        
      case PushSumSend(bossRef) =>
//         if(deadNeighbourList.length == numOfNeighbours && !isTerminated()){
//          deadNeighbourList = -1::deadNeighbourList
//          println("This node " + index + " is dead, cannot execute!")
//       //   context.stop(self)
//          bossRef ! FinishPushSum(index, s / w, flag = -1)
//        }
//    //      println("This node " + index + " is dead, cannot execute!")
//         if(!isTerminated() && deadNeighbourList.length < numOfNeighbours){	//this node must be infected before
//           var i :Int = ranNeighbourIndex()
//           var sValue :Double = s
//           var wValue :Double = w
//           s /= 2
//           w /= 2
////           assignRoundValue()
//           myNeighbourRef(i) ! PushSumReceive(sValue / 2, wValue / 2, bossRef)
//        }
//        if(isTerminated()){
//          
//          println("This node " + index + " is terminated, with the s/w value " + (s / w))
//          bossRef ! FinishPushSum(index, s / w, flag = 1)
//        }
        
      case PushSumRemoveNeighbour(reIndex, sValue, wValue, bossRef) =>
//        s += sValue
//        w += wValue
        var tRef = List[ActorRef]()
        var tIndex = List[Int]()
        println(this.index)
        println(reIndex)
        println(myNeighbourIndex)
        for(i <- 0 to (myNeighbourIndex.length - 1)){
          if(myNeighbourIndex(i) != reIndex){
            tRef = myNeighbourRef(i)::tRef
            tIndex = myNeighbourIndex(i)::tIndex
          }
          else{    
        	numOfNeighbours -= 1
        	println(myNeighbourIndex(i))
          }
        }
        myNeighbourRef = tRef
        myNeighbourIndex = tIndex
//        sender ! PushSumPass(sValue, wValue, bossRef, flag = 1)
//        println(myNeighbourIndex)
        
      case PushSumPass(sValue, wValue, bossRef, flag) =>
        if(isTerminated()){
//          for(i <- 0 to (myNeighbourIndex.length - 1))
          if(flag == 1)
            bossRef ! FinishPushSum1(index, s/w, flag = 1)
          sender ! PushSumRemoveNeighbour(this.index, sValue, wValue, bossRef)
          sender ! PushSumPass(sValue, wValue, bossRef, flag = 1)
//          println("The index " + this.index + " terminates, with the s/w value " + (s / w))
//          println("a")
        }
        else{
          s += sValue
          w += wValue
          if(myNeighbourRef.length == 0){
            bossRef ! FinishPushSum1(index, s/w, flag = 1)
            println("b")
//            context.stop(self)
          }
          else if(isTerminated()){
            println("c")
            println("The index " + index + " terminates, with the s/w value " + (s / w))
            sender ! PushSumRemoveNeighbour(this.index, sValue, wValue, bossRef)
            sender ! PushSumPass(sValue, wValue, bossRef, flag = 1)
            bossRef ! FinishPushSum1(index, s / w, flag = 0)
            var i :Int = ranNeighbourIndex()
            s /= 2
            w /= 2
//            myNeighbourRef(i) ! PushSumPass(s, w, bossRef, flag = 0)
          }
          else{
            println("d")
            assignRoundValue()
            var i :Int = ranNeighbourIndex()
            println("The index " + index + " has s/w value " + (s / w))
            s /= 2
            w /= 2
            myNeighbourRef(i) ! PushSumPass(s, w, bossRef, flag = 0)
          }
        }
          
      case SendMSG(sValue, wValue, bossRef) =>
        if(stop){
          sender ! ReNeighbour(this.index, bossRef)
        }
        else{
          s += sValue
          w += wValue
          if(isTerminated()){
            sender ! ReNeighbour(this.index, bossRef)
            this.stop = true
          }
          else{
          }
          self ! ReSend(bossRef)
        }
          
      case ReNeighbour(reIndex, bossRef) =>
        var tRef = List[ActorRef]()
        var tIndex = List[Int]()
        println(this.index)
        println(reIndex)
        println(myNeighbourIndex)
        for(i <- 0 to (myNeighbourIndex.length - 1)){
          if(myNeighbourIndex(i) != reIndex){
            tRef = myNeighbourRef(i)::tRef
            tIndex = myNeighbourIndex(i)::tIndex
          }
          else{    
        	numOfNeighbours -= 1
//        	println(myNeighbourIndex(i))
          }
        }
        myNeighbourRef = tRef
        myNeighbourIndex = tIndex
        
        if(myNeighbourIndex.length == 0){
          bossRef ! FinishPushSum1(index, s / w, flag = 0)
          println("The MSG ends at node " + index + " with sw value " + s/w)
          this.stop = true
        }
        else{
          self ! ReSend(bossRef)
        }
        
      case ReSend(bossRef) =>
        if(!this.stop){
          println("The node " + index + " has sw value " + s/w)
          var i :Int = ranNeighbourIndex()
          s /= 2
          w /= 2
          myNeighbourRef(i) ! SendMSG(s, w, bossRef)
        }

        
      case Report =>
        println("The node " + index + " has the sw value " + s/w)
        
      case _ =>
        println("Unknown Message to Worker Actor!")
    }
    
  }
  
  def start(numOfNodes :Int, topology :String, algorithm :String){
    
    val system = ActorSystem("Project2System")
    
    val master = system.actorOf(Props(new Master(numOfNodes, topology, algorithm)), name = "master")
    
    master ! Start
    
  }
}