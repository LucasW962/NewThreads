--[[

    Thread Module for thread control
    Author -- Yoda962 (yoda_06)
    Constucted 4/14/25

    --// This module is part of yoda962's framework
    Which will later include server/client communcations

    --// functions marked as local are PRIVATE and should be script bounded
    any other method is public 

    This script uses a custom promise i made called OnError which returns a callback if an error occurs

]]

local Threads = {}
Threads.__index = Threads

local ActiveThreads = {} --// STORES THE ACTIVE THREADS BEING USED
local ThreadCount = 0 --// TOTAL THREADS USED
local ThreadStats = { --// LOCAL VARIABLE FOR THREAD STATS, IMPORTANT DATA WILL BE STORED LOCALLY THEN CALLED IN FUNCTIONS, AS UPVALS ARE 32 + 28(N) BYTES, FASTER THAN GLOBALS
	completed = 0,
	failed = 0,
	created = 0,
	peak = 0,
	averageRuntime = 0,
	totalRuntime = 0,
}

local Middleware = { --// ANOTHER LOCAL TABLE, USED FOR STORING IMPORTANT MIDDLEWARE DATA
	beforeStart = {},
	afterComplete = {},
	onError = {},
	onPause = {},
	onResume = {},
}

--// ALL MY TYPES BELOW
export type InitList = {
	Completed: { [string]: number },
	Failed: { [string]: number },
	FailedList: { [string]: string },
	Total: { [number]: number },
}

export type ThreadType = {
	id: number,
	active: boolean,
	thread: thread?,
	status: string,
	result: any?,
	paused: boolean,
	error: any?,
	metadata: { [string]: any },
	startTime: number?,
	endTime: number?,
	timeout: number?,
	completeCallback: ((result: any?) -> ())?,
	errorCallback: ((error: any) -> ())?,

	Start: <T...>(self: ThreadType, callback: (T...) -> any, T...) -> (boolean, string?),
	Kill: (self: ThreadType) -> (boolean, string?),
	Pause: (self: ThreadType) -> (boolean, string?),
	Resume: (self: ThreadType) -> (boolean, string?),
	Wait: (self: ThreadType) -> (any?, any?),
	CheckPause: (self: ThreadType, args: any?) -> (),
	WithTimeout: (self: ThreadType, seconds: number) -> ThreadType,
	OnComplete: (self: ThreadType, callback: (result: any?) -> ()) -> ThreadType,
	OnError: (self: ThreadType, callback: (error: any) -> ()) -> ThreadType,
	WithMetadata: (self: ThreadType, key: string, value: any) -> ThreadType,
}

export type ThreadsStatic = {
	new: () -> ThreadType,
	GetActive: () -> { [number]: ThreadType },
	KillAll: () -> (),
	RegisterMiddleware: <T...>(hook: string, callback: (T...) -> any) -> number,
	GetStats: () -> { [string]: number },
}

--// PRIVATE METHODS START HERE
local function setupTimeout(self: ThreadType)
	if not self.timeout then
		return
	end --// IF NO TIMEOUT NUM WAS GIVEN, WE RETURN
	task.delay(self.timeout, function() --// DELAY THE TIMEOUT
		if not self.active then
			return
		end --// IF IT WASNT ACTIVE AFTER THE DELAY ENDS, WE CANT CONTINUE
		Threads.Kill(self) --// END THE THREAD
		if not self.errorCallback then
			return
		end --// IF THERES NO CALLBACK FOR THE ERROR JUST RETURN
		self.errorCallback("Thread timed out after " .. self.timeout .. " seconds") --// CALLBACK WITH THE ERROR, STATING THE TIME IT TIME OUTED FOR
	end)
end

local function markAsRunning(self: ThreadType) --// PRIVATE FUNCTION TO SAY THE THREAD IS ACTIVE
	self.active = true
	self.status = "running"
end

local function recordCompletion(self: ThreadType) --// USED TO RECORD THE COMPLETION WITH TIME, ADDS ONTO STATS
	self.endTime = os.clock()
	local runTime = self.endTime - (self.startTime or 0)

	ThreadStats.totalRuntime += runTime --// ADD IT TO OUR STATS, UPVAL CALL FASTER THAN A GLOBAL TABLE CALL
	ThreadStats.averageRuntime = ThreadStats.totalRuntime / ThreadStats.created
end

local function handleSuccess(self: ThreadType, result: any) --// USED FOR WHEN SUCCESS HAPPENS
	self.result = result
	self.status = "completed"
	ThreadStats.completed += 1

	for _, middlewareFunc in Middleware.afterComplete do
		middlewareFunc(self, result) --// IF THERES A MIDDLEWARE FUNC INSIDE ITS COMPLETETION, WE CALL IT WITH THE THREAD AND RESULT
	end

	if self.completeCallback then
		task.spawn(function()
			self.completeCallback(result) --// USE MULTITASKING THREAD FOR THE CALLBACK, DOESNT YIELD OUR MAIN THREAD
		end)
	end
end

local function handleError(self: ThreadType, error: any) --// ANOTHER PRIVATE FUNCTION FOR HANDLING ERRORS
	self.error = error
	self.status = "error"
	ThreadStats.failed += 1 --// ADD ONTO OUR LOCAL STATS

	for _, middlewareFunc in Middleware.onError do
		middlewareFunc(self, error) --// IF THERES A FUNCTION INSIDE ONERROR, CALL IT WITH THE THREAD AND ERROR REASON AS PARAMS
	end

	if self.errorCallback then
		task.spawn(function()
			self.errorCallback(error) --// MULITTASK THE ERROR WITH THE CALLBACK IF THERE WAS A PROVIDED ONE
		end)
	end
end

local function cleanupThread(self: ThreadType) --// PRIVATE FUNC TO END THE THREAD
	self.active = false
	ActiveThreads[self.id] = nil
end

local function updatePeakStats()
	local activeCount = 0
	for _ in ActiveThreads do
		activeCount += 1
	end --// UPDATE ACTIVE COUNT, USED FOR STAT TRACKING
	ThreadStats.peak = math.max(ThreadStats.peak, activeCount)
end

--// PUBLIC CONSTRUCTOR STARTS HERE
function Threads.new(): ThreadType
	local self = setmetatable({}, Threads) :: ThreadType
	self.id = ThreadCount + 1 --// SET THE ID TO A UNIQUE ONE, BASICALLY INDEX IT BASED ON THE TOTAL THREADS FOR THE LOCAL UPVAL DATA
	self.active = false
	self.thread = nil
	self.status = "idle"
	self.result = nil --// ALL THESE NIL VALUES AE LIKELY TO BE USED LATER
	self.paused = false
	self.error = nil
	self.metadata = {}
	self.timeout = nil
	self.completeCallback = nil
	self.errorCallback = nil
	ThreadCount += 1
	ThreadStats.created += 1 --// ADD TO THREAD STATS
	return self
end

function Threads.WithTimeout(
	self: ThreadType,
	seconds: number
): ThreadType --// IF THIS METHOD IS CALLED, BASICALLY IF IT DOESNT COMPLETE, AKA YIELD FOR THE TIMEOUT TIME, IT WILL CALLBACK WITH ITS ERROR
	self.timeout = seconds
	return self
end

function Threads.OnComplete(self: ThreadType, callback: (result: any?) -> ()): ThreadType --// SAME THING LIKE TIMEOUT, BUT WHEN ITS COMPLETED AND NOT YIELDED, IT CALLBACKS WITH ITS COMPLETE FUNC
	self.completeCallback = callback
	return self
end

function Threads.OnError(self: ThreadType, callback: (error: any) -> ()): ThreadType --// THIS FUNC GOES HAND TO HAND WITH TIMEOUT, BASICALLY IF TIMEOUT RETURNS TRUE, THIS ERROR CALLBACK HAPPENS
	self.errorCallback = callback
	return self
end

function Threads.WithMetadata(self: ThreadType, key: string, value: any): ThreadType --// ADD METADATA TO THE TABLE, USEFUL FOR TRACKING WHAT THE THREADS ARE ACTIVELY BEING USED FOR
	self.metadata[key] = value
	return self
end

function Threads.Start<T...>(self: ThreadType, callback: (T...) -> any, ...: T...): (boolean, string?) --// START THE THREAD, USE TYPE PACKING FOR OUR CALLBACKS
	if self.active then
		return false, "Thread is already running"
	end --// CANT START THE THREAD MORE THAN ONCE

	local args = { ... } --// I USED A TABLE HERE, I COULD HAVE USED SELECT INCASE #ARGS == 0, BUT ITS LIKELY FOR THE CALLBACK TO BE A FUNCTION, SO I FOUND IT POINTLESS TO USE SELECT HERE

	for _, middlewareFunc in Middleware.beforeStart do
		middlewareFunc(self) --// INCASE THE MIDDLEWARE HAS A BEFORE START FUNCTION, WE CALL IT BEFORE THE THREAD STARTS
	end

	self.startTime = os.clock() --// GET THE START TIME FOR OUR TIMEOUT IF WE USED IT

	setupTimeout(self) --// START TIMEOUT

	ActiveThreads[self.id] = self
	updatePeakStats()

	self.thread = task.spawn(function() --// SET THE THREAD AS A NEW TASK.SPAWN
		markAsRunning(self) --// THREAD IS NOW RUNNING, TELL OUR PRIVATE FUNC IT IS

		local success, result = pcall(function()
			return callback(table.unpack(args)) --// UNPACK THE ARGS WITH THE CALLBACK, USED FOR TRACKING COMPLETION AND RUNNING THE FUNCTION PROVIDED
		end)

		recordCompletion(self)

		if success then
			handleSuccess(self, result) --// SUCCESS HAPPENED, RECORD IT
		else
			handleError(self, result) --// ERROR HAPPENED, RECORD IT
		end

		cleanupThread(self) --// KILL THE THREAD NOW THAT IS DONE
	end)

	return true --// USED TO HELP ME DETERMINE ITS DONE
end

function Threads.Kill(self: ThreadType): (boolean, string?) --// KILL THE THREAD
	if not self.active or not self.thread then
		return false, "Thread is not running"
	end --// USED TO CHECK IF THE THREAD IS ACTUALLY ACTIVE OR NOT

	task.cancel(self.thread) --// CANCEL THE THREAD
	self.active = false
	self.status = "killed"
	ActiveThreads[self.id] = nil --// REMOVE IT FROM ACTIVE THREADS TABLE AS IT GOT CANCELLED
	return true
end

function Threads.Pause(self: ThreadType): (boolean, string?)
	if not self.active then
		return false, "Thread is not running"
	end
	self.paused = true
	self.status = "paused"

	for _, middlewareFunc in Middleware.onPause do
		middlewareFunc(self) --// CALL THE MIDDLEWARE FUNC IF A CALLBACK  WAS PROVIDED FOR PAUSING
	end

	return true
end

function Threads.Resume(self: ThreadType): (boolean, string?)
	if not self.paused then
		return false, "Thread is not paused"
	end
	self.paused = false
	self.status = "running"

	for _, middlewareFunc in Middleware.onResume do
		middlewareFunc(self) --// CALLBACK IF THERE WAS A RESUME FUNC
	end

	return true
end

function Threads.Wait(self: ThreadType): (any?, any?) --// USED TO PAUSE THE THREAD
	while self.active do coroutine.yield() end
	return self.result, self.error
end

function Threads.CheckPause(self: ThreadType, args: any?) --// WAITS WHILE THE THREAD IS PAUSED
	while self.paused do coroutine.yield() end
end


function Threads.GetActive(): { [number]: ThreadType } --// HELPER FUNCTION TO RETURN ALL ACTIVE THREADS
	return ActiveThreads
end

function Threads.KillAll()
	for id, thread in ActiveThreads do --// A SIMPLE LOOP TO KILL ALL THE ACTIVE THREADS
		Threads.Kill(thread)
	end
	ActiveThreads = {} --// EMPTY TABLE AFTER ALL THREADS ARE DEAD
end

function Threads.RegisterMiddleware<T...>(hook: string, callback: (T...) -> any): number --// FUNCMTION FOR ADDING MIDDLEWARE FUNCS
	if not Middleware[hook] then
		return 0
	end

	table.insert(Middleware[hook], callback) --// INSERT IT INSIDE MIDDLEWARE, WITH ITS HOOK AND CALLBACK
	return #Middleware[hook]
end

function Threads.GetStats(): { [string]: number } --// RETURN ALL THE THREAD STATUS
	return ThreadStats
end

return Threads
