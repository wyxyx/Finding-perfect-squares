#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open System.Collections.Generic
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

let system = System.create "MySystem" <| Configuration.load ()

type InData = InData of calid: int * start: int * k: int * N: int
type MessageParent =
    | InitialData of N:int * k: int
    | Res of calid: int * res: List<int> * k: int * N: int

let mutable start = 1

let strategy =
    Strategy.OneForOne (fun e ->
        match e with
        | _ -> SupervisorStrategy.DefaultDecider.Decide(e))


let Worker (mailbox: Actor<_>) = 
    let rec loop() =
        actor {
            let! InData(calid, start, k, N) = mailbox.Receive()
            let sender = mailbox.Sender()
            let x = bigint start
            let judge = new List<int>()
            let mutable calres = bigint 0 
            for i = bigint 0 to bigint (k-1) do
                calres <- calres + ((x+i)*(x+i))
            let sqr = bigint (sqrt (float calres))
            if sqr*sqr = calres then
                judge.Add(start)
            let final = Res(calid, judge, k, N)
            sender <! final
            return! loop()
        }
    loop()

let Boss (mailbox: Actor<_>) =
    let rec loop() =
        actor {
            let! msg = mailbox.Receive()
            match msg with
            | InitialData(N, k) ->
                let workerArray = Array.create 13 (spawn system "Worker" Worker)
                {0..12} |> Seq.iter (fun a ->
                    workerArray.[a] <- spawn system (string a) Worker
                    ()
                    if start <= N then
                        workerArray.[a] <! InData(a, start, k, N)
                        start <- start + 1
                )
            | Res(calid, res, k, N) -> 
                for ele in res do
                    printf "%i " ele
                if start <= N then
                    mailbox.Sender() <! InData(calid, start, k, N)
                    start <- start + 1
                

            return! loop()
        } 
    loop()

let boss = spawnOpt system "Boss" Boss [ SupervisorStrategy(strategy) ] 
async{
    let nums : string array = fsi.CommandLineArgs |> Array.tail
    let N = int nums.[0]
    let k = int nums.[1]
    let mutable delay = 100
    if N <= 10000 then
        delay <- 100
    elif N <= 100000 then
        delay <- 500
    elif N <= 1000000 then
        delay <- 3000
    elif N <= 10000000 then
        delay <- 20000
    else
        delay <- 200000
    boss <! InitialData(N,k)
    System.Threading.Thread.Sleep delay
} |> Async.RunSynchronously
system.Terminate()