use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};
type DynFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

static WAKE_TIMES: Mutex<BTreeMap<Instant, Vec<Waker>>> = Mutex::new(BTreeMap::new());
static NEW_TASKS: Mutex<Vec<DynFuture>> = Mutex::new(Vec::new());

struct Foo {
    n: u64,
    started: bool,
    sleep: Pin<Box<Sleep>>,
}

impl Future for Foo {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.started {
            println!("Start: {}", self.n);
            self.started = true;
        }

        if self.sleep.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }

        println!("End: {}", self.n);
        Poll::Ready(())
    }
}

struct Sleep {
    wake_time: Instant,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if Instant::now() >= self.wake_time {
            Poll::Ready(())
        } else {
            let mut wake_times = WAKE_TIMES.lock().unwrap();
            let wakers_vec = wake_times.entry(self.wake_time).or_default();
            wakers_vec.push(cx.waker().clone());
            Poll::Pending
        }
    }
}

struct JoinAll<F> {
    futures: Vec<Pin<Box<F>>>,
}

impl<F: Future> Future for JoinAll<F> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let is_pending = |future: &mut Pin<Box<F>>| future.as_mut().poll(cx).is_pending();
        self.futures.retain_mut(is_pending);

        if self.futures.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

fn foo(n: u64) -> Foo {
    let duration = Duration::from_millis(n * 100);
    let sleep = Box::pin(sleep(duration));
    Foo {
        n,
        started: false,
        sleep,
    }
}

fn sleep(duration: Duration) -> Sleep {
    let wake_time = Instant::now() + duration;
    Sleep { wake_time }
}

// fn join_all<F: Future>(futures: Vec<F>) -> JoinAll<F> {
//     JoinAll {
//         futures: futures.into_iter().map(Box::pin).collect(),
//     }
// }

enum JoinState<T> {
    Unawaited,
    Awaited(Waker),
    Ready(T),
    Done,
}

struct JoinHandle<T> {
    state: Arc<Mutex<JoinState<T>>>,
}

// We await JoinHandle to wait for task to finish, so JoinHandle needs to implement Future
impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut guard = self.state.lock().unwrap();

        match std::mem::replace(&mut *guard, JoinState::Done) {
            JoinState::Ready(v) => Poll::Ready(v),
            JoinState::Unawaited | JoinState::Awaited(_) => {
                // replace the previous waker, if any
                *guard = JoinState::Awaited(cx.waker().clone());
                Poll::Pending
            }
            JoinState::Done => unreachable!("Polled again after Ready"),
        }
    }
}

struct AwakeFlag(Mutex<bool>);

impl Wake for AwakeFlag {
    fn wake(self: Arc<Self>) {
        *self.0.lock().unwrap() = true;
    }
}

async fn wrap_with_join_state<F: Future>(future: F, join_state: Arc<Mutex<JoinState<F::Output>>>) {
    let value = future.await;
    let mut guard = join_state.lock().unwrap();

    let old_state = std::mem::replace(&mut *guard, JoinState::Ready(value));
    drop(guard);

    if let JoinState::Awaited(waker) = old_state {
        waker.wake_by_ref();
    }
    // *guard = JoinState::Ready(value)
}

fn spawn<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let join_state = Arc::new(Mutex::new(JoinState::Unawaited));
    let join_handle = JoinHandle {
        state: Arc::clone(&join_state),
    };

    let task = Box::pin(wrap_with_join_state(future, join_state));
    NEW_TASKS.lock().unwrap().push(task);

    join_handle
}

async fn async_main() {
    let k = 10;
    let mut task_handles = Vec::new();
    for n in 1..=k {
        task_handles.push(spawn(foo(n)));
    }

    for handle in task_handles {
        handle.await;
    }
}

fn main() {
    let awake_flag = Arc::new(AwakeFlag(Mutex::new(false)));
    let waker = Waker::from(Arc::clone(&awake_flag));
    let mut cx = Context::from_waker(&waker);
    let mut main_task = Box::pin(async_main());
    let mut other_tasks: Vec<DynFuture> = Vec::new();

    loop {
        *awake_flag.0.lock().unwrap() = false;

        if main_task.as_mut().poll(&mut cx).is_ready() {
            return;
        }

        let is_pending = |task: &mut DynFuture| task.as_mut().poll(&mut cx).is_pending();
        other_tasks.retain_mut(is_pending);

        loop {
            let Some(mut task) = NEW_TASKS.lock().unwrap().pop() else {
                break;
            };

            if task.as_mut().poll(&mut cx).is_pending() {
                other_tasks.push(task);
            }
        }

        if *awake_flag.0.lock().unwrap() {
            continue;
        }

        if other_tasks.is_empty() {
            break;
        }

        let mut wake_times = WAKE_TIMES.lock().unwrap();
        let next_wake = wake_times.keys().next().expect("sleep forever?");
        thread::sleep(next_wake.saturating_duration_since(Instant::now()));

        while let Some(entry) = wake_times.first_entry()
            && *entry.key() <= Instant::now()
        {
            entry.remove().into_iter().for_each(Waker::wake);
        }
    }
}
