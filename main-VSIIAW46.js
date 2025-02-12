var JS=Object.defineProperty,QS=Object.defineProperties;var ew=Object.getOwnPropertyDescriptors;var Rc=Object.getOwnPropertySymbols;var fv=Object.prototype.hasOwnProperty,hv=Object.prototype.propertyIsEnumerable;var dv=(n,e,t)=>e in n?JS(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,be=(n,e)=>{for(var t in e||={})fv.call(e,t)&&dv(n,t,e[t]);if(Rc)for(var t of Rc(e))hv.call(e,t)&&dv(n,t,e[t]);return n},Mt=(n,e)=>QS(n,ew(e));var pv=(n,e)=>{var t={};for(var i in n)fv.call(n,i)&&e.indexOf(i)<0&&(t[i]=n[i]);if(n!=null&&Rc)for(var i of Rc(n))e.indexOf(i)<0&&hv.call(n,i)&&(t[i]=n[i]);return t};var _i=(n,e,t)=>new Promise((i,r)=>{var s=c=>{try{a(t.next(c))}catch(l){r(l)}},o=c=>{try{a(t.throw(c))}catch(l){r(l)}},a=c=>c.done?i(c.value):Promise.resolve(c.value).then(s,o);a((t=t.apply(n,e)).next())});function tw(n,e){return Object.is(n,e)}var Ot=null,Nc=!1,Rf=1,Ss=Symbol("SIGNAL");function gt(n){let e=Ot;return Ot=n,e}function gv(){return Ot}var Pc={version:0,lastCleanEpoch:0,dirty:!1,producerNode:void 0,producerLastReadVersion:void 0,producerIndexOfThis:void 0,nextProducerIndex:0,liveConsumerNode:void 0,liveConsumerIndexOfThis:void 0,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function vv(n){if(Nc)throw new Error("");if(Ot===null)return;Ot.consumerOnSignalRead(n);let e=Ot.nextProducerIndex++;if(Lc(Ot),e<Ot.producerNode.length&&Ot.producerNode[e]!==n&&Yo(Ot)){let t=Ot.producerNode[e];Oc(t,Ot.producerIndexOfThis[e])}Ot.producerNode[e]!==n&&(Ot.producerNode[e]=n,Ot.producerIndexOfThis[e]=Yo(Ot)?xv(n,Ot,e):0),Ot.producerLastReadVersion[e]=n.version}function nw(){Rf++}function iw(n){if(!(Yo(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Rf)){if(!n.producerMustRecompute(n)&&!Pf(n)){mv(n);return}n.producerRecomputeValue(n),mv(n)}}function yv(n){if(n.liveConsumerNode===void 0)return;let e=Nc;Nc=!0;try{for(let t of n.liveConsumerNode)t.dirty||sw(t)}finally{Nc=e}}function rw(){return Ot?.consumerAllowSignalWrites!==!1}function sw(n){n.dirty=!0,yv(n),n.consumerMarkedDirty?.(n)}function mv(n){n.dirty=!1,n.lastCleanEpoch=Rf}function Nf(n){return n&&(n.nextProducerIndex=0),gt(n)}function _v(n,e){if(gt(e),!(!n||n.producerNode===void 0||n.producerIndexOfThis===void 0||n.producerLastReadVersion===void 0)){if(Yo(n))for(let t=n.nextProducerIndex;t<n.producerNode.length;t++)Oc(n.producerNode[t],n.producerIndexOfThis[t]);for(;n.producerNode.length>n.nextProducerIndex;)n.producerNode.pop(),n.producerLastReadVersion.pop(),n.producerIndexOfThis.pop()}}function Pf(n){Lc(n);for(let e=0;e<n.producerNode.length;e++){let t=n.producerNode[e],i=n.producerLastReadVersion[e];if(i!==t.version||(iw(t),i!==t.version))return!0}return!1}function Of(n){if(Lc(n),Yo(n))for(let e=0;e<n.producerNode.length;e++)Oc(n.producerNode[e],n.producerIndexOfThis[e]);n.producerNode.length=n.producerLastReadVersion.length=n.producerIndexOfThis.length=0,n.liveConsumerNode&&(n.liveConsumerNode.length=n.liveConsumerIndexOfThis.length=0)}function xv(n,e,t){if(Ev(n),n.liveConsumerNode.length===0&&Mv(n))for(let i=0;i<n.producerNode.length;i++)n.producerIndexOfThis[i]=xv(n.producerNode[i],n,i);return n.liveConsumerIndexOfThis.push(t),n.liveConsumerNode.push(e)-1}function Oc(n,e){if(Ev(n),n.liveConsumerNode.length===1&&Mv(n))for(let i=0;i<n.producerNode.length;i++)Oc(n.producerNode[i],n.producerIndexOfThis[i]);let t=n.liveConsumerNode.length-1;if(n.liveConsumerNode[e]=n.liveConsumerNode[t],n.liveConsumerIndexOfThis[e]=n.liveConsumerIndexOfThis[t],n.liveConsumerNode.length--,n.liveConsumerIndexOfThis.length--,e<n.liveConsumerNode.length){let i=n.liveConsumerIndexOfThis[e],r=n.liveConsumerNode[e];Lc(r),r.producerIndexOfThis[i]=e}}function Yo(n){return n.consumerIsAlwaysLive||(n?.liveConsumerNode?.length??0)>0}function Lc(n){n.producerNode??=[],n.producerIndexOfThis??=[],n.producerLastReadVersion??=[]}function Ev(n){n.liveConsumerNode??=[],n.liveConsumerIndexOfThis??=[]}function Mv(n){return n.producerNode!==void 0}function ow(){throw new Error}var Sv=ow;function aw(){Sv()}function wv(n){Sv=n}var cw=null;function bv(n,e){rw()||aw(),n.equal(n.value,e)||(n.value=e,lw(n))}var Tv=Mt(be({},Pc),{equal:tw,value:void 0,kind:"signal"});function lw(n){n.version++,nw(),yv(n),cw?.()}function He(n){return typeof n=="function"}function ws(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Fc=ws(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Zo(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var Rt=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(He(i))try{i()}catch(s){e=s instanceof Fc?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{Cv(s)}catch(o){e=e??[],o instanceof Fc?e=[...e,...o.errors]:e.push(o)}}if(e)throw new Fc(e)}}add(e){var t;if(e&&e!==this)if(this.closed)Cv(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Zo(t,e)}remove(e){let{_finalizers:t}=this;t&&Zo(t,e),e instanceof n&&e._removeParent(this)}};Rt.EMPTY=(()=>{let n=new Rt;return n.closed=!0,n})();var Lf=Rt.EMPTY;function kc(n){return n instanceof Rt||n&&"closed"in n&&He(n.remove)&&He(n.add)&&He(n.unsubscribe)}function Cv(n){He(n)?n():n.unsubscribe()}var Wn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var bs={setTimeout(n,e,...t){let{delegate:i}=bs;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=bs;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function Uc(n){bs.setTimeout(()=>{let{onUnhandledError:e}=Wn;if(e)e(n);else throw n})}function Ko(){}var Dv=Ff("C",void 0,void 0);function Av(n){return Ff("E",void 0,n)}function Iv(n){return Ff("N",n,void 0)}function Ff(n,e,t){return{kind:n,value:e,error:t}}var Dr=null;function Ts(n){if(Wn.useDeprecatedSynchronousErrorHandling){let e=!Dr;if(e&&(Dr={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=Dr;if(Dr=null,t)throw i}}else n()}function Rv(n){Wn.useDeprecatedSynchronousErrorHandling&&Dr&&(Dr.errorThrown=!0,Dr.error=n)}var Ar=class extends Rt{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,kc(e)&&e.add(this)):this.destination=fw}static create(e,t,i){return new Cs(e,t,i)}next(e){this.isStopped?Uf(Iv(e),this):this._next(e)}error(e){this.isStopped?Uf(Av(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?Uf(Dv,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},uw=Function.prototype.bind;function kf(n,e){return uw.call(n,e)}var Bf=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){Bc(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){Bc(i)}else Bc(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){Bc(t)}}},Cs=class extends Ar{constructor(e,t,i){super();let r;if(He(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&Wn.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&kf(e.next,s),error:e.error&&kf(e.error,s),complete:e.complete&&kf(e.complete,s)}):r=e}this.destination=new Bf(r)}};function Bc(n){Wn.useDeprecatedSynchronousErrorHandling?Rv(n):Uc(n)}function dw(n){throw n}function Uf(n,e){let{onStoppedNotification:t}=Wn;t&&bs.setTimeout(()=>t(n,e))}var fw={closed:!0,next:Ko,error:dw,complete:Ko};var Ds=typeof Symbol=="function"&&Symbol.observable||"@@observable";function fn(n){return n}function Vf(...n){return zf(n)}function zf(n){return n.length===0?fn:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var vt=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=pw(t)?t:new Cs(t,i,r);return Ts(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=Nv(i),new i((r,s)=>{let o=new Cs({next:a=>{try{t(a)}catch(c){s(c),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[Ds](){return this}pipe(...t){return zf(t)(this)}toPromise(t){return t=Nv(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function Nv(n){var e;return(e=n??Wn.Promise)!==null&&e!==void 0?e:Promise}function hw(n){return n&&He(n.next)&&He(n.error)&&He(n.complete)}function pw(n){return n&&n instanceof Ar||hw(n)&&kc(n)}function Hf(n){return He(n?.lift)}function rt(n){return e=>{if(Hf(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function st(n,e,t,i,r){return new Gf(n,e,t,i,r)}var Gf=class extends Ar{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};function As(){return rt((n,e)=>{let t=null;n._refCount++;let i=st(e,void 0,void 0,void 0,()=>{if(!n||n._refCount<=0||0<--n._refCount){t=null;return}let r=n._connection,s=t;t=null,r&&(!s||r===s)&&r.unsubscribe(),e.unsubscribe()});n.subscribe(i),i.closed||(t=n.connect())})}var Is=class extends vt{constructor(e,t){super(),this.source=e,this.subjectFactory=t,this._subject=null,this._refCount=0,this._connection=null,Hf(e)&&(this.lift=e.lift)}_subscribe(e){return this.getSubject().subscribe(e)}getSubject(){let e=this._subject;return(!e||e.isStopped)&&(this._subject=this.subjectFactory()),this._subject}_teardown(){this._refCount=0;let{_connection:e}=this;this._subject=this._connection=null,e?.unsubscribe()}connect(){let e=this._connection;if(!e){e=this._connection=new Rt;let t=this.getSubject();e.add(this.source.subscribe(st(t,void 0,()=>{this._teardown(),t.complete()},i=>{this._teardown(),t.error(i)},()=>this._teardown()))),e.closed&&(this._connection=null,e=Rt.EMPTY)}return e}refCount(){return As()(this)}};var Pv=ws(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Yt=(()=>{class n extends vt{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new Vc(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new Pv}next(t){Ts(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){Ts(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){Ts(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?Lf:(this.currentObservers=null,s.push(t),new Rt(()=>{this.currentObservers=null,Zo(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new vt;return t.source=this,t}}return n.create=(e,t)=>new Vc(e,t),n})(),Vc=class extends Yt{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:Lf}};var Wt=class extends Yt{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};var an=new vt(n=>n.complete());function Ov(n){return n&&He(n.schedule)}function Lv(n){return n[n.length-1]}function Fv(n){return He(Lv(n))?n.pop():void 0}function qi(n){return Ov(Lv(n))?n.pop():void 0}function Uv(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{l(i.next(u))}catch(d){o(d)}}function c(u){try{l(i.throw(u))}catch(d){o(d)}}function l(u){u.done?s(u.value):r(u.value).then(a,c)}l((i=i.apply(n,e||[])).next())})}function kv(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function Ir(n){return this instanceof Ir?(this.v=n,this):new Ir(n)}function Bv(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(h){return function(g){return Promise.resolve(g).then(h,d)}}function a(h,g){i[h]&&(r[h]=function(v){return new Promise(function(m,p){s.push([h,v,m,p])>1||c(h,v)})},g&&(r[h]=g(r[h])))}function c(h,g){try{l(i[h](g))}catch(v){f(s[0][3],v)}}function l(h){h.value instanceof Ir?Promise.resolve(h.value.v).then(u,d):f(s[0][2],h)}function u(h){c("next",h)}function d(h){c("throw",h)}function f(h,g){h(g),s.shift(),s.length&&c(s[0][0],s[0][1])}}function Vv(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof kv=="function"?kv(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,c){o=n[s](o),r(a,c,o.done,o.value)})}}function r(s,o,a,c){Promise.resolve(c).then(function(l){s({value:l,done:a})},o)}}var zc=n=>n&&typeof n.length=="number"&&typeof n!="function";function Hc(n){return He(n?.then)}function Gc(n){return He(n[Ds])}function jc(n){return Symbol.asyncIterator&&He(n?.[Symbol.asyncIterator])}function Wc(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function mw(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var $c=mw();function qc(n){return He(n?.[$c])}function Xc(n){return Bv(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield Ir(t.read());if(r)return yield Ir(void 0);yield yield Ir(i)}}finally{t.releaseLock()}})}function Yc(n){return He(n?.getReader)}function Vt(n){if(n instanceof vt)return n;if(n!=null){if(Gc(n))return gw(n);if(zc(n))return vw(n);if(Hc(n))return yw(n);if(jc(n))return zv(n);if(qc(n))return _w(n);if(Yc(n))return xw(n)}throw Wc(n)}function gw(n){return new vt(e=>{let t=n[Ds]();if(He(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function vw(n){return new vt(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function yw(n){return new vt(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,Uc)})}function _w(n){return new vt(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function zv(n){return new vt(e=>{Ew(n,e).catch(t=>e.error(t))})}function xw(n){return zv(Xc(n))}function Ew(n,e){var t,i,r,s;return Uv(this,void 0,void 0,function*(){try{for(t=Vv(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function cn(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function Zc(n,e=0){return rt((t,i)=>{t.subscribe(st(i,r=>cn(i,n,()=>i.next(r),e),()=>cn(i,n,()=>i.complete(),e),r=>cn(i,n,()=>i.error(r),e)))})}function Kc(n,e=0){return rt((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function Hv(n,e){return Vt(n).pipe(Kc(e),Zc(e))}function Gv(n,e){return Vt(n).pipe(Kc(e),Zc(e))}function jv(n,e){return new vt(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function Wv(n,e){return new vt(t=>{let i;return cn(t,e,()=>{i=n[$c](),cn(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>He(i?.return)&&i.return()})}function Jc(n,e){if(!n)throw new Error("Iterable cannot be null");return new vt(t=>{cn(t,e,()=>{let i=n[Symbol.asyncIterator]();cn(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function $v(n,e){return Jc(Xc(n),e)}function qv(n,e){if(n!=null){if(Gc(n))return Hv(n,e);if(zc(n))return jv(n,e);if(Hc(n))return Gv(n,e);if(jc(n))return Jc(n,e);if(qc(n))return Wv(n,e);if(Yc(n))return $v(n,e)}throw Wc(n)}function Nt(n,e){return e?qv(n,e):Vt(n)}function ze(...n){let e=qi(n);return Nt(n,e)}function Rs(n,e){let t=He(n)?n:()=>n,i=r=>r.error(t());return new vt(e?r=>e.schedule(i,0,r):i)}function jf(n){return!!n&&(n instanceof vt||He(n.lift)&&He(n.subscribe))}var xi=ws(n=>function(){n(this),this.name="EmptyError",this.message="no elements in sequence"});function nt(n,e){return rt((t,i)=>{let r=0;t.subscribe(st(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:Mw}=Array;function Sw(n,e){return Mw(e)?n(...e):n(e)}function Xv(n){return nt(e=>Sw(n,e))}var{isArray:ww}=Array,{getPrototypeOf:bw,prototype:Tw,keys:Cw}=Object;function Yv(n){if(n.length===1){let e=n[0];if(ww(e))return{args:e,keys:null};if(Dw(e)){let t=Cw(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function Dw(n){return n&&typeof n=="object"&&bw(n)===Tw}function Zv(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function Qc(...n){let e=qi(n),t=Fv(n),{args:i,keys:r}=Yv(n);if(i.length===0)return Nt([],e);let s=new vt(Aw(i,e,r?o=>Zv(r,o):fn));return t?s.pipe(Xv(t)):s}function Aw(n,e,t=fn){return i=>{Kv(e,()=>{let{length:r}=n,s=new Array(r),o=r,a=r;for(let c=0;c<r;c++)Kv(e,()=>{let l=Nt(n[c],e),u=!1;l.subscribe(st(i,d=>{s[c]=d,u||(u=!0,a--),a||i.next(t(s.slice()))},()=>{--o||i.complete()}))},i)},i)}}function Kv(n,e,t){n?cn(t,n,e):e()}function Jv(n,e,t,i,r,s,o,a){let c=[],l=0,u=0,d=!1,f=()=>{d&&!c.length&&!l&&e.complete()},h=v=>l<i?g(v):c.push(v),g=v=>{s&&e.next(v),l++;let m=!1;Vt(t(v,u++)).subscribe(st(e,p=>{r?.(p),s?h(p):e.next(p)},()=>{m=!0},void 0,()=>{if(m)try{for(l--;c.length&&l<i;){let p=c.shift();o?cn(e,o,()=>g(p)):g(p)}f()}catch(p){e.error(p)}}))};return n.subscribe(st(e,h,()=>{d=!0,f()})),()=>{a?.()}}function Lt(n,e,t=1/0){return He(e)?Lt((i,r)=>nt((s,o)=>e(i,s,r,o))(Vt(n(i,r))),t):(typeof e=="number"&&(t=e),rt((i,r)=>Jv(i,r,n,t)))}function Wf(n=1/0){return Lt(fn,n)}function Qv(){return Wf(1)}function Ns(...n){return Qv()(Nt(n,qi(n)))}function el(n){return new vt(e=>{Vt(n()).subscribe(e)})}function Rn(n,e){return rt((t,i)=>{let r=0;t.subscribe(st(i,s=>n.call(e,s,r++)&&i.next(s)))})}function Xi(n){return rt((e,t)=>{let i=null,r=!1,s;i=e.subscribe(st(t,void 0,void 0,o=>{s=Vt(n(o,Xi(n)(e))),i?(i.unsubscribe(),i=null,s.subscribe(t)):r=!0})),r&&(i.unsubscribe(),i=null,s.subscribe(t))})}function ey(n,e,t,i,r){return(s,o)=>{let a=t,c=e,l=0;s.subscribe(st(o,u=>{let d=l++;c=a?n(c,u,d):(a=!0,u),i&&o.next(c)},r&&(()=>{a&&o.next(c),o.complete()})))}}function Rr(n,e){return He(e)?Lt(n,e,1):Lt(n,1)}function Yi(n){return rt((e,t)=>{let i=!1;e.subscribe(st(t,r=>{i=!0,t.next(r)},()=>{i||t.next(n),t.complete()}))})}function Ei(n){return n<=0?()=>an:rt((e,t)=>{let i=0;e.subscribe(st(t,r=>{++i<=n&&(t.next(r),n<=i&&t.complete())}))})}function tl(n=Iw){return rt((e,t)=>{let i=!1;e.subscribe(st(t,r=>{i=!0,t.next(r)},()=>i?t.complete():t.error(n())))})}function Iw(){return new xi}function Ps(n){return rt((e,t)=>{try{e.subscribe(t)}finally{t.add(n)}})}function Mi(n,e){let t=arguments.length>=2;return i=>i.pipe(n?Rn((r,s)=>n(r,s,i)):fn,Ei(1),t?Yi(e):tl(()=>new xi))}function Os(n){return n<=0?()=>an:rt((e,t)=>{let i=[];e.subscribe(st(t,r=>{i.push(r),n<i.length&&i.shift()},()=>{for(let r of i)t.next(r);t.complete()},void 0,()=>{i=null}))})}function $f(n,e){let t=arguments.length>=2;return i=>i.pipe(n?Rn((r,s)=>n(r,s,i)):fn,Os(1),t?Yi(e):tl(()=>new xi))}function qf(n,e){return rt(ey(n,e,arguments.length>=2,!0))}function Xf(...n){let e=qi(n);return rt((t,i)=>{(e?Ns(n,t,e):Ns(n,t)).subscribe(i)})}function Nn(n,e){return rt((t,i)=>{let r=null,s=0,o=!1,a=()=>o&&!r&&i.complete();t.subscribe(st(i,c=>{r?.unsubscribe();let l=0,u=s++;Vt(n(c,u)).subscribe(r=st(i,d=>i.next(e?e(c,d,u,l++):d),()=>{r=null,a()}))},()=>{o=!0,a()}))})}function Yf(n){return rt((e,t)=>{Vt(n).subscribe(st(t,()=>t.complete(),Ko)),!t.closed&&e.subscribe(t)})}function Ft(n,e,t){let i=He(n)||e||t?{next:n,error:e,complete:t}:n;return i?rt((r,s)=>{var o;(o=i.subscribe)===null||o===void 0||o.call(i);let a=!0;r.subscribe(st(s,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),s.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),s.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),s.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):fn}var eh={JSACTION:"jsaction"},th={JSACTION:"__jsaction",OWNER:"__owner"},ry={};function Rw(n){return n[th.JSACTION]}function ty(n,e){n[th.JSACTION]=e}function Nw(n){return ry[n]}function Pw(n,e){ry[n]=e}var Ne={AUXCLICK:"auxclick",CHANGE:"change",CLICK:"click",CLICKMOD:"clickmod",CLICKONLY:"clickonly",DBLCLICK:"dblclick",FOCUS:"focus",FOCUSIN:"focusin",BLUR:"blur",FOCUSOUT:"focusout",SUBMIT:"submit",KEYDOWN:"keydown",KEYPRESS:"keypress",KEYUP:"keyup",MOUSEUP:"mouseup",MOUSEDOWN:"mousedown",MOUSEOVER:"mouseover",MOUSEOUT:"mouseout",MOUSEENTER:"mouseenter",MOUSELEAVE:"mouseleave",MOUSEMOVE:"mousemove",POINTERUP:"pointerup",POINTERDOWN:"pointerdown",POINTEROVER:"pointerover",POINTEROUT:"pointerout",POINTERENTER:"pointerenter",POINTERLEAVE:"pointerleave",POINTERMOVE:"pointermove",POINTERCANCEL:"pointercancel",GOTPOINTERCAPTURE:"gotpointercapture",LOSTPOINTERCAPTURE:"lostpointercapture",ERROR:"error",LOAD:"load",UNLOAD:"unload",TOUCHSTART:"touchstart",TOUCHEND:"touchend",TOUCHMOVE:"touchmove",INPUT:"input",SCROLL:"scroll",TOGGLE:"toggle",CUSTOM:"_custom"},Ow=[Ne.MOUSEENTER,Ne.MOUSELEAVE,"pointerenter","pointerleave"],Mz=[Ne.CLICK,Ne.DBLCLICK,Ne.FOCUSIN,Ne.FOCUSOUT,Ne.KEYDOWN,Ne.KEYUP,Ne.KEYPRESS,Ne.MOUSEOVER,Ne.MOUSEOUT,Ne.SUBMIT,Ne.TOUCHSTART,Ne.TOUCHEND,Ne.TOUCHMOVE,"touchcancel","auxclick","change","compositionstart","compositionupdate","compositionend","beforeinput","input","select","copy","cut","paste","mousedown","mouseup","wheel","contextmenu","dragover","dragenter","dragleave","drop","dragstart","dragend","pointerdown","pointermove","pointerup","pointercancel","pointerover","pointerout","gotpointercapture","lostpointercapture","ended","loadedmetadata","pagehide","pageshow","visibilitychange","beforematch"],Lw=[Ne.FOCUS,Ne.BLUR,Ne.ERROR,Ne.LOAD,Ne.TOGGLE],nh=n=>Lw.indexOf(n)>=0;var Fw=3,kw=13,Uw=32,hn={MAC_ENTER:Fw,ENTER:kw,SPACE:Uw};function Bw(n){return n===Ne.MOUSEENTER?Ne.MOUSEOVER:n===Ne.MOUSELEAVE?Ne.MOUSEOUT:n===Ne.POINTERENTER?Ne.POINTEROVER:n===Ne.POINTERLEAVE?Ne.POINTEROUT:n}function Vw(n,e,t,i){let r=!1;nh(e)&&(r=!0);let s=typeof i=="boolean"?{capture:r,passive:i}:r;return n.addEventListener(e,t,s),{eventType:e,handler:t,capture:r,passive:i}}function zw(n,e){if(n.removeEventListener){let t=typeof e.passive=="boolean"?{capture:e.capture}:e.capture;n.removeEventListener(e.eventType,e.handler,t)}else n.detachEvent&&n.detachEvent(`on${e.eventType}`,e.handler)}function Hw(n){n.preventDefault?n.preventDefault():n.returnValue=!1}var ny=typeof navigator<"u"&&/Macintosh/.test(navigator.userAgent);function Gw(n){return n.which===2||n.which==null&&n.button===4}function jw(n){return ny&&n.metaKey||!ny&&n.ctrlKey||Gw(n)||n.shiftKey}var Sz=typeof navigator<"u"&&!/Opera/.test(navigator.userAgent)&&/WebKit/.test(navigator.userAgent),wz=typeof navigator<"u"&&(/MSIE/.test(navigator.userAgent)||/Trident/.test(navigator.userAgent)),bz=typeof navigator<"u"&&!/Opera|WebKit/.test(navigator.userAgent)&&/Gecko/.test(navigator.product);function Ww(n,e,t){let i=n.relatedTarget;return(n.type===Ne.MOUSEOVER&&e===Ne.MOUSEENTER||n.type===Ne.MOUSEOUT&&e===Ne.MOUSELEAVE||n.type===Ne.POINTEROVER&&e===Ne.POINTERENTER||n.type===Ne.POINTEROUT&&e===Ne.POINTERLEAVE)&&(!i||i!==t&&!t.contains(i))}function $w(n,e){let t={};for(let i in n){if(i==="srcElement"||i==="target")continue;let r=i,s=n[r];typeof s!="function"&&(t[r]=s)}return n.type===Ne.MOUSEOVER?t.type=Ne.MOUSEENTER:n.type===Ne.MOUSEOUT?t.type=Ne.MOUSELEAVE:n.type===Ne.POINTEROVER?t.type=Ne.POINTERENTER:t.type=Ne.POINTERLEAVE,t.target=t.srcElement=e,t.bubbles=!1,t._originalEvent=n,t}var Tz={Enter:hn.ENTER," ":hn.SPACE},Cz={A:hn.ENTER,BUTTON:0,CHECKBOX:hn.SPACE,COMBOBOX:hn.ENTER,FILE:0,GRIDCELL:hn.ENTER,LINK:hn.ENTER,LISTBOX:hn.ENTER,MENU:0,MENUBAR:0,MENUITEM:0,MENUITEMCHECKBOX:0,MENUITEMRADIO:0,OPTION:0,RADIO:hn.SPACE,RADIOGROUP:hn.SPACE,RESET:0,SUBMIT:0,SWITCH:hn.SPACE,TAB:0,TREE:hn.ENTER,TREEITEM:hn.ENTER};var qw=typeof navigator<"u"&&/iPhone|iPad|iPod/.test(navigator.userAgent),sl=class{element;handlerInfos=[];constructor(e){this.element=e}addEventListener(e,t,i){qw&&(this.element.style.cursor="pointer"),this.handlerInfos.push(Vw(this.element,e,t(this.element),i))}cleanUp(){for(let e=0;e<this.handlerInfos.length;e++)zw(this.element,this.handlerInfos[e]);this.handlerInfos=[]}},Xw={NAMESPACE_ACTION_SEPARATOR:".",EVENT_ACTION_SEPARATOR:":"};function Zi(n){return n.eventType}function ih(n,e){n.eventType=e}function il(n){return n.event}function sy(n,e){n.event=e}function oy(n){return n.targetElement}function ay(n,e){n.targetElement=e}function cy(n){return n.eic}function Yw(n,e){n.eic=e}function Zw(n){return n.timeStamp}function Kw(n,e){n.timeStamp=e}function rl(n){return n.eia}function ly(n,e,t){n.eia=[e,t]}function Zf(n){n.eia=void 0}function nl(n){return n[1]}function Jw(n){return n.eirp}function uy(n,e){n.eirp=e}function dy(n){return n.eir}function fy(n,e){n.eir=e}function hy(n){return{eventType:n.eventType,event:n.event,targetElement:n.targetElement,eic:n.eic,eia:n.eia,timeStamp:n.timeStamp,eirp:n.eirp,eiack:n.eiack,eir:n.eir}}function Qw(n,e,t,i,r,s,o,a){return{eventType:n,event:e,targetElement:t,eic:i,timeStamp:r,eia:s,eirp:o,eiack:a}}var Kf=class n{eventInfo;constructor(e){this.eventInfo=e}getEventType(){return Zi(this.eventInfo)}setEventType(e){ih(this.eventInfo,e)}getEvent(){return il(this.eventInfo)}setEvent(e){sy(this.eventInfo,e)}getTargetElement(){return oy(this.eventInfo)}setTargetElement(e){ay(this.eventInfo,e)}getContainer(){return cy(this.eventInfo)}setContainer(e){Yw(this.eventInfo,e)}getTimestamp(){return Zw(this.eventInfo)}setTimestamp(e){Kw(this.eventInfo,e)}getAction(){let e=rl(this.eventInfo);if(e)return{name:e[0],element:e[1]}}setAction(e){if(!e){Zf(this.eventInfo);return}ly(this.eventInfo,e.name,e.element)}getIsReplay(){return Jw(this.eventInfo)}setIsReplay(e){uy(this.eventInfo,e)}getResolved(){return dy(this.eventInfo)}setResolved(e){fy(this.eventInfo,e)}clone(){return new n(hy(this.eventInfo))}},eb={},tb=/\s*;\s*/,nb=Ne.CLICK,Jf=class{a11yClickSupport=!1;clickModSupport=!0;syntheticMouseEventSupport;updateEventInfoForA11yClick=void 0;preventDefaultForA11yClick=void 0;populateClickOnlyAction=void 0;constructor({syntheticMouseEventSupport:e=!1,clickModSupport:t=!0}={}){this.syntheticMouseEventSupport=e,this.clickModSupport=t}resolveEventType(e){this.clickModSupport&&Zi(e)===Ne.CLICK&&jw(il(e))?ih(e,Ne.CLICKMOD):this.a11yClickSupport&&this.updateEventInfoForA11yClick(e)}resolveAction(e){dy(e)||(this.populateAction(e,oy(e)),fy(e,!0))}resolveParentAction(e){let t=rl(e),i=t&&nl(t);Zf(e);let r=i&&this.getParentNode(i);r&&this.populateAction(e,r)}populateAction(e,t){let i=t;for(;i&&i!==cy(e)&&(i.nodeType===Node.ELEMENT_NODE&&this.populateActionOnElement(i,e),!rl(e));)i=this.getParentNode(i);let r=rl(e);if(r&&(this.a11yClickSupport&&this.preventDefaultForA11yClick(e),this.syntheticMouseEventSupport&&(Zi(e)===Ne.MOUSEENTER||Zi(e)===Ne.MOUSELEAVE||Zi(e)===Ne.POINTERENTER||Zi(e)===Ne.POINTERLEAVE)))if(Ww(il(e),Zi(e),nl(r))){let s=$w(il(e),nl(r));sy(e,s),ay(e,nl(r))}else Zf(e)}getParentNode(e){let t=e[th.OWNER];if(t)return t;let i=e.parentNode;return i?.nodeName==="#document-fragment"?i?.host??null:i}populateActionOnElement(e,t){let i=this.parseActions(e),r=i[Zi(t)];r!==void 0&&ly(t,r,e),this.a11yClickSupport&&this.populateClickOnlyAction(e,t,i)}parseActions(e){let t=Rw(e);if(!t){let i=e.getAttribute(eh.JSACTION);if(!i)t=eb,ty(e,t);else{if(t=Nw(i),!t){t={};let r=i.split(tb);for(let s=0;s<r.length;s++){let o=r[s];if(!o)continue;let a=o.indexOf(Xw.EVENT_ACTION_SEPARATOR),c=a!==-1,l=c?o.substr(0,a).trim():nb,u=c?o.substr(a+1).trim():o;t[l]=u}Pw(i,t)}ty(e,t)}}return t}addA11yClickSupport(e,t,i){this.a11yClickSupport=!0,this.updateEventInfoForA11yClick=e,this.preventDefaultForA11yClick=t,this.populateClickOnlyAction=i}},py=function(n){return n[n.I_AM_THE_JSACTION_FRAMEWORK=0]="I_AM_THE_JSACTION_FRAMEWORK",n}(py||{}),Qf=class{dispatchDelegate;actionResolver;eventReplayer;eventReplayScheduled=!1;replayEventInfoWrappers=[];constructor(e,{actionResolver:t,eventReplayer:i}={}){this.dispatchDelegate=e,this.actionResolver=t,this.eventReplayer=i}dispatch(e){let t=new Kf(e);this.actionResolver?.resolveEventType(e),this.actionResolver?.resolveAction(e);let i=t.getAction();if(i&&ib(i.element,t)&&Hw(t.getEvent()),this.eventReplayer&&t.getIsReplay()){this.scheduleEventInfoWrapperReplay(t);return}this.dispatchDelegate(t)}scheduleEventInfoWrapperReplay(e){this.replayEventInfoWrappers.push(e),!this.eventReplayScheduled&&(this.eventReplayScheduled=!0,Promise.resolve().then(()=>{this.eventReplayScheduled=!1,this.eventReplayer(this.replayEventInfoWrappers)}))}};function ib(n,e){return n.tagName==="A"&&(e.getEventType()===Ne.CLICK||e.getEventType()===Ne.CLICKMOD)}var my=Symbol.for("propagationStopped"),rh={REPLAY:101};var rb="`preventDefault` called during event replay.";var sb="`composedPath` called during event replay.",ol=class{dispatchDelegate;clickModSupport;actionResolver;dispatcher;constructor(e,t=!0){this.dispatchDelegate=e,this.clickModSupport=t,this.actionResolver=new Jf({clickModSupport:t}),this.dispatcher=new Qf(i=>{this.dispatchToDelegate(i)},{actionResolver:this.actionResolver})}dispatch(e){this.dispatcher.dispatch(e)}dispatchToDelegate(e){for(e.getIsReplay()&&cb(e),ob(e);e.getAction();){if(lb(e),nh(e.getEventType())&&e.getAction().element!==e.getTargetElement()||(this.dispatchDelegate(e.getEvent(),e.getAction().name),ab(e)))return;this.actionResolver.resolveParentAction(e.eventInfo)}}};function ob(n){let e=n.getEvent(),t=n.getEvent().stopPropagation.bind(e),i=()=>{e[my]=!0,t()};Nr(e,"stopPropagation",i),Nr(e,"stopImmediatePropagation",i)}function ab(n){return!!n.getEvent()[my]}function cb(n){let e=n.getEvent(),t=n.getTargetElement(),i=e.preventDefault.bind(e);Nr(e,"target",t),Nr(e,"eventPhase",rh.REPLAY),Nr(e,"preventDefault",()=>{throw i(),new Error(rb+"")}),Nr(e,"composedPath",()=>{throw new Error(sb+"")})}function lb(n){let e=n.getEvent(),t=n.getAction()?.element;t&&Nr(e,"currentTarget",t,{configurable:!0})}function Nr(n,e,t,{configurable:i=!1}={}){Object.defineProperty(n,e,{value:t,configurable:i})}function gy(n,e){n.ecrd(t=>{e.dispatch(t)},py.I_AM_THE_JSACTION_FRAMEWORK)}function ub(n){return n?.q??[]}function db(n){n&&(iy(n.c,n.et,n.h),iy(n.c,n.etc,n.h,!0))}function iy(n,e,t,i){for(let r=0;r<e.length;r++)n.removeEventListener(e[r],t,i)}var fb=!1,vy=(()=>{class n{static MOUSE_SPECIAL_SUPPORT=fb;containerManager;eventHandlers={};browserEventTypeToExtraEventTypes={};dispatcher=null;queuedEventInfos=[];constructor(t){this.containerManager=t}handleEvent(t,i,r){let s=Qw(t,i,i.target,r,Date.now());this.handleEventInfo(s)}handleEventInfo(t){if(!this.dispatcher){uy(t,!0),this.queuedEventInfos?.push(t);return}this.dispatcher(t)}addEvent(t,i,r){if(t in this.eventHandlers||!this.containerManager||!n.MOUSE_SPECIAL_SUPPORT&&Ow.indexOf(t)>=0)return;let s=(a,c,l)=>{this.handleEvent(a,c,l)};this.eventHandlers[t]=s;let o=Bw(i||t);if(o!==t){let a=this.browserEventTypeToExtraEventTypes[o]||[];a.push(t),this.browserEventTypeToExtraEventTypes[o]=a}this.containerManager.addEventListener(o,a=>c=>{s(t,c,a)},r)}replayEarlyEvents(t=window._ejsa){t&&(this.replayEarlyEventInfos(t.q),db(t),delete window._ejsa)}replayEarlyEventInfos(t){for(let i=0;i<t.length;i++){let r=t[i],s=this.getEventTypesForBrowserEventType(r.eventType);for(let o=0;o<s.length;o++){let a=hy(r);ih(a,s[o]),this.handleEventInfo(a)}}}getEventTypesForBrowserEventType(t){let i=[];return this.eventHandlers[t]&&i.push(t),this.browserEventTypeToExtraEventTypes[t]&&i.push(...this.browserEventTypeToExtraEventTypes[t]),i}handler(t){return this.eventHandlers[t]}cleanUp(){this.containerManager?.cleanUp(),this.containerManager=null,this.eventHandlers={},this.browserEventTypeToExtraEventTypes={},this.dispatcher=null,this.queuedEventInfos=[]}registerDispatcher(t,i){this.ecrd(t,i)}ecrd(t,i){if(this.dispatcher=t,this.queuedEventInfos?.length){for(let r=0;r<this.queuedEventInfos.length;r++)this.handleEventInfo(this.queuedEventInfos[r]);this.queuedEventInfos=null}}}return n})();function yy(n,e=window){return ub(e._ejsas?.[n])}function sh(n,e=window){e._ejsas&&(e._ejsas[n]=void 0)}var Pe=class extends Error{code;constructor(e,t){super(kl(e,t)),this.code=e}};function kl(n,e){return`${`NG0${Math.abs(n)}`}${e?": "+e:""}`}var d_=Symbol("InputSignalNode#UNSET"),hb=Mt(be({},Tv),{transformFn:void 0,applyValueToInputSignal(n,e){bv(n,e)}});function f_(n,e){let t=Object.create(hb);t.value=n,t.transformFn=e?.transform;function i(){if(vv(t),t.value===d_)throw new Pe(-950,!1);return t.value}return i[Ss]=t,i}function Yh(n){return{toString:n}.toString()}function xt(n){for(let e in n)if(n[e]===xt)return e;throw Error("Could not find renamed property on target object.")}function gn(n){if(typeof n=="string")return n;if(Array.isArray(n))return"["+n.map(gn).join(", ")+"]";if(n==null)return""+n;if(n.overriddenName)return`${n.overriddenName}`;if(n.name)return`${n.name}`;let e=n.toString();if(e==null)return""+e;let t=e.indexOf(`
`);return t===-1?e:e.substring(0,t)}function _y(n,e){return n==null||n===""?e===null?"":e:e==null||e===""?n:n+" "+e}var pb=xt({__forward_ref__:xt});function h_(n){return n.__forward_ref__=h_,n.toString=function(){return gn(this())},n}function On(n){return p_(n)?n():n}function p_(n){return typeof n=="function"&&n.hasOwnProperty(pb)&&n.__forward_ref__===h_}function ke(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Ul(n){return xy(n,g_)||xy(n,v_)}function m_(n){return Ul(n)!==null}function xy(n,e){return n.hasOwnProperty(e)?n[e]:null}function mb(n){let e=n&&(n[g_]||n[v_]);return e||null}function Ey(n){return n&&(n.hasOwnProperty(My)||n.hasOwnProperty(gb))?n[My]:null}var g_=xt({\u0275prov:xt}),My=xt({\u0275inj:xt}),v_=xt({ngInjectableDef:xt}),gb=xt({ngInjectorDef:xt}),Ae=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=ke({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function y_(n){return n&&!!n.\u0275providers}var vb=xt({\u0275cmp:xt}),yb=xt({\u0275dir:xt}),_b=xt({\u0275pipe:xt}),xb=xt({\u0275mod:xt}),gl=xt({\u0275fac:xt}),ta=xt({__NG_ELEMENT_ID__:xt}),Sy=xt({__NG_ENV_ID__:xt});function Eb(n){return typeof n=="string"?n:n==null?"":String(n)}function Mb(n){return typeof n=="function"?n.name||n.toString():typeof n=="object"&&n!=null&&typeof n.type=="function"?n.type.name||n.type.toString():Eb(n)}function Sb(n,e){let t=e?`. Dependency path: ${e.join(" > ")} > ${n}`:"";throw new Pe(-200,n)}function Zh(n,e){throw new Pe(-201,!1)}var Ke=function(n){return n[n.Default=0]="Default",n[n.Host=1]="Host",n[n.Self=2]="Self",n[n.SkipSelf=4]="SkipSelf",n[n.Optional=8]="Optional",n}(Ke||{}),gh;function __(){return gh}function Pn(n){let e=gh;return gh=n,e}function x_(n,e,t){let i=Ul(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&Ke.Optional)return null;if(e!==void 0)return e;Zh(n,"Injector")}var wb={},na=wb,bb="__NG_DI_FLAG__",vl="ngTempTokenPath",Tb="ngTokenPath",Cb=/\n/gm,Db="\u0275",wy="__source",Vs;function Ab(){return Vs}function Ki(n){let e=Vs;return Vs=n,e}function Ib(n,e=Ke.Default){if(Vs===void 0)throw new Pe(-203,!1);return Vs===null?x_(n,void 0,e):Vs.get(n,e&Ke.Optional?null:void 0,e)}function et(n,e=Ke.Default){return(__()||Ib)(On(n),e)}function Q(n,e=Ke.Default){return et(n,Bl(e))}function Bl(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function vh(n){let e=[];for(let t=0;t<n.length;t++){let i=On(n[t]);if(Array.isArray(i)){if(i.length===0)throw new Pe(900,!1);let r,s=Ke.Default;for(let o=0;o<i.length;o++){let a=i[o],c=Rb(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(et(r,s))}else e.push(et(i))}return e}function Rb(n){return n[bb]}function Nb(n,e,t,i){let r=n[vl];throw e[wy]&&r.unshift(e[wy]),n.message=Pb(`
`+n.message,r,t,i),n[Tb]=r,n[vl]=null,n}function Pb(n,e,t,i=null){n=n&&n.charAt(0)===`
`&&n.charAt(1)==Db?n.slice(2):n;let r=gn(e);if(Array.isArray(e))r=e.map(gn).join(" -> ");else if(typeof e=="object"){let s=[];for(let o in e)if(e.hasOwnProperty(o)){let a=e[o];s.push(o+":"+(typeof a=="string"?JSON.stringify(a):gn(a)))}r=`{${s.join(", ")}}`}return`${t}${i?"("+i+")":""}[${r}]: ${n.replace(Cb,`
  `)}`}function js(n,e){let t=n.hasOwnProperty(gl);return t?n[gl]:null}function Kh(n,e){n.forEach(t=>Array.isArray(t)?Kh(t,e):e(t))}function E_(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function yl(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}var ia={},Ws=[],Qi=new Ae(""),M_=new Ae("",-1),S_=new Ae(""),_l=class{get(e,t=na){if(t===na){let i=new Error(`NullInjectorError: No provider for ${gn(e)}!`);throw i.name="NullInjectorError",i}return t}};function w_(n,e){let t=n[xb]||null;if(!t&&e===!0)throw new Error(`Type ${gn(n)} does not have '\u0275mod' property.`);return t}function er(n){return n[vb]||null}function Jh(n){return n[yb]||null}function Qh(n){return n[_b]||null}function b_(n){let e=er(n)||Jh(n)||Qh(n);return e!==null&&e.standalone}function Br(n){return{\u0275providers:n}}function Ob(...n){return{\u0275providers:ep(!0,n),\u0275fromNgModule:!0}}function ep(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return Kh(e,o=>{let a=o;yh(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&T_(r,s),t}function T_(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];tp(r,s=>{e(s,i)})}}function yh(n,e,t,i){if(n=On(n),!n)return!1;let r=null,s=Ey(n),o=!s&&er(n);if(!s&&!o){let c=n.ngModule;if(s=Ey(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)yh(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;try{Kh(s.imports,u=>{yh(u,e,t,i)&&(l||=[],l.push(u))})}finally{}l!==void 0&&T_(l,e)}if(!a){let l=js(r)||(()=>new r);e({provide:r,useFactory:l,deps:Ws},r),e({provide:S_,useValue:r,multi:!0},r),e({provide:Qi,useValue:()=>et(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;tp(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function tp(n,e){for(let t of n)y_(t)&&(t=t.\u0275providers),Array.isArray(t)?tp(t,e):e(t)}var Lb=xt({provide:String,useValue:xt});function C_(n){return n!==null&&typeof n=="object"&&Lb in n}function Fb(n){return!!(n&&n.useExisting)}function kb(n){return!!(n&&n.useFactory)}function _h(n){return typeof n=="function"}var Vl=new Ae(""),ll={},Ub={},oh;function np(){return oh===void 0&&(oh=new _l),oh}var _n=class{},ra=class extends _n{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,Eh(e,o=>this.processProvider(o)),this.records.set(M_,Ls(void 0,this)),r.has("environment")&&this.records.set(_n,Ls(void 0,this));let s=this.records.get(Vl);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(S_,Ws,Ke.Self))}destroy(){Qo(this),this._destroyed=!0;let e=gt(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),gt(e)}}onDestroy(e){return Qo(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Qo(this);let t=Ki(this),i=Pn(void 0),r;try{return e()}finally{Ki(t),Pn(i)}}get(e,t=na,i=Ke.Default){if(Qo(this),e.hasOwnProperty(Sy))return e[Sy](this);i=Bl(i);let r,s=Ki(this),o=Pn(void 0);try{if(!(i&Ke.SkipSelf)){let c=this.records.get(e);if(c===void 0){let l=jb(e)&&Ul(e);l&&this.injectableDefInScope(l)?c=Ls(xh(e),ll):c=null,this.records.set(e,c)}if(c!=null)return this.hydrate(e,c)}let a=i&Ke.Self?np():this.parent;return t=i&Ke.Optional&&t===na?null:t,a.get(e,t)}catch(a){if(a.name==="NullInjectorError"){if((a[vl]=a[vl]||[]).unshift(gn(e)),s)throw a;return Nb(a,e,"R3InjectorError",this.source)}else throw a}finally{Pn(o),Ki(s)}}resolveInjectorInitializers(){let e=gt(null),t=Ki(this),i=Pn(void 0),r;try{let s=this.get(Qi,Ws,Ke.Self);for(let o of s)o()}finally{Ki(t),Pn(i),gt(e)}}toString(){let e=[],t=this.records;for(let i of t.keys())e.push(gn(i));return`R3Injector[${e.join(", ")}]`}processProvider(e){e=On(e);let t=_h(e)?e:On(e&&e.provide),i=Vb(e);if(!_h(e)&&e.multi===!0){let r=this.records.get(t);r||(r=Ls(void 0,ll,!0),r.factory=()=>vh(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t){let i=gt(null);try{return t.value===ll&&(t.value=Ub,t.value=t.factory()),typeof t.value=="object"&&t.value&&Gb(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{gt(i)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=On(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function xh(n){let e=Ul(n),t=e!==null?e.factory:js(n);if(t!==null)return t;if(n instanceof Ae)throw new Pe(204,!1);if(n instanceof Function)return Bb(n);throw new Pe(204,!1)}function Bb(n){if(n.length>0)throw new Pe(204,!1);let t=mb(n);return t!==null?()=>t.factory(n):()=>new n}function Vb(n){if(C_(n))return Ls(void 0,n.useValue);{let e=zb(n);return Ls(e,ll)}}function zb(n,e,t){let i;if(_h(n)){let r=On(n);return js(r)||xh(r)}else if(C_(n))i=()=>On(n.useValue);else if(kb(n))i=()=>n.useFactory(...vh(n.deps||[]));else if(Fb(n))i=()=>et(On(n.useExisting));else{let r=On(n&&(n.useClass||n.provide));if(Hb(n))i=()=>new r(...vh(n.deps));else return js(r)||xh(r)}return i}function Qo(n){if(n.destroyed)throw new Pe(205,!1)}function Ls(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function Hb(n){return!!n.deps}function Gb(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function jb(n){return typeof n=="function"||typeof n=="object"&&n instanceof Ae}function Eh(n,e){for(let t of n)Array.isArray(t)?Eh(t,e):t&&y_(t)?Eh(t.\u0275providers,e):e(t)}function Fn(n,e){n instanceof ra&&Qo(n);let t,i=Ki(n),r=Pn(void 0);try{return e()}finally{Ki(i),Pn(r)}}function D_(){return __()!==void 0||Ab()!=null}function Wb(n){if(!D_())throw new Pe(-203,!1)}function $b(n){return typeof n=="function"}var xn=0,qe=1,$e=2,$t=3,qn=4,Xn=5,ri=6,by=7,Si=8,tr=9,nr=10,Zt=11,sa=12,Ty=13,ua=14,Ln=15,$s=16,Fs=17,qs=18,zl=19,A_=20,Ji=21,ah=22,xl=23,vn=24,nn=25,I_=1,Or=6,wi=7,El=8,Ml=9,yn=10;function ii(n){return Array.isArray(n)&&typeof n[I_]=="object"}function oi(n){return Array.isArray(n)&&n[I_]===!0}function R_(n){return(n.flags&4)!==0}function Js(n){return n.componentOffset>-1}function qb(n){return(n.flags&1)===1}function Qs(n){return!!n.template}function oa(n){return(n[$e]&512)!==0}function eo(n){return(n[$e]&256)===256}var Mh=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function N_(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var ip=(()=>{let n=()=>P_;return n.ngInherit=!0,n})();function P_(n){return n.type.prototype.ngOnChanges&&(n.setInput=Yb),Xb}function Xb(){let n=L_(this),e=n?.current;if(e){let t=n.previous;if(t===ia)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function Yb(n,e,t,i,r){let s=this.declaredInputs[i],o=L_(n)||Zb(n,{previous:ia,current:null}),a=o.current||(o.current={}),c=o.previous,l=c[s];a[s]=new Mh(l&&l.currentValue,t,c===ia),N_(n,e,r,t)}var O_="__ngSimpleChanges__";function L_(n){return n[O_]||null}function Zb(n,e){return n[O_]=e}var Cy=null;var ti=function(n,e,t){Cy?.(n,e,t)},Kb="svg",Jb="math";function bi(n){for(;Array.isArray(n);)n=n[xn];return n}function Di(n,e){return bi(e[n.index])}function rp(n,e){return n.data[e]}function Lr(n,e){let t=e[n];return ii(t)?t:t[xn]}function sp(n){return(n[$e]&128)===128}function Qb(n){return oi(n[$t])}function Dy(n,e){return e==null?null:n[e]}function F_(n){n[Fs]=0}function op(n){n[$e]&1024||(n[$e]|=1024,sp(n)&&Gl(n))}function Hl(n){return!!(n[$e]&9216||n[vn]?.dirty)}function Sh(n){n[nr].changeDetectionScheduler?.notify(9),n[$e]&64&&(n[$e]|=1024),Hl(n)&&Gl(n)}function Gl(n){n[nr].changeDetectionScheduler?.notify(0);let e=Fr(n);for(;e!==null&&!(e[$e]&8192||(e[$e]|=8192,!sp(e)));)e=Fr(e)}function k_(n,e){if(eo(n))throw new Pe(911,!1);n[Ji]===null&&(n[Ji]=[]),n[Ji].push(e)}function eT(n,e){if(n[Ji]===null)return;let t=n[Ji].indexOf(e);t!==-1&&n[Ji].splice(t,1)}function Fr(n){let e=n[$t];return oi(e)?e[$t]:e}var yt={lFrame:$_(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var wh=!1;function tT(){return yt.lFrame.elementDepthCount}function nT(){yt.lFrame.elementDepthCount++}function iT(){yt.lFrame.elementDepthCount--}function U_(){return yt.bindingsEnabled}function da(){return yt.skipHydrationRootTNode!==null}function rT(n){return yt.skipHydrationRootTNode===n}function sT(n){yt.skipHydrationRootTNode=n}function oT(){yt.skipHydrationRootTNode=null}function Ti(){return yt.lFrame.lView}function B_(){return yt.lFrame.tView}function Vr(){let n=V_();for(;n!==null&&n.type===64;)n=n.parent;return n}function V_(){return yt.lFrame.currentTNode}function aT(){let n=yt.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function ap(n,e){let t=yt.lFrame;t.currentTNode=n,t.isParent=e}function z_(){return yt.lFrame.isParent}function cT(){yt.lFrame.isParent=!1}function H_(){return wh}function Ay(n){let e=wh;return wh=n,e}function lT(n){return yt.lFrame.bindingIndex=n}function uT(){return yt.lFrame.inI18n}function dT(n,e){let t=yt.lFrame;t.bindingIndex=t.bindingRootIndex=n,bh(e)}function fT(){return yt.lFrame.currentDirectiveIndex}function bh(n){yt.lFrame.currentDirectiveIndex=n}function G_(n){yt.lFrame.currentQueryIndex=n}function hT(n){let e=n[qe];return e.type===2?e.declTNode:e.type===1?n[Xn]:null}function j_(n,e,t){if(t&Ke.SkipSelf){let r=e,s=n;for(;r=r.parent,r===null&&!(t&Ke.Host);)if(r=hT(s),r===null||(s=s[ua],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=yt.lFrame=W_();return i.currentTNode=e,i.lView=n,!0}function cp(n){let e=W_(),t=n[qe];yt.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function W_(){let n=yt.lFrame,e=n===null?null:n.child;return e===null?$_(n):e}function $_(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function q_(){let n=yt.lFrame;return yt.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var X_=q_;function lp(){let n=q_();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function pT(){return yt.lFrame.selectedIndex}function kr(n){yt.lFrame.selectedIndex=n}function Y_(){return yt.lFrame.currentNamespace}var Z_=!0;function mT(){return Z_}function sr(n){Z_=n}function gT(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=P_(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function vT(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function ul(n,e,t){K_(n,e,3,t)}function dl(n,e,t,i){(n[$e]&3)===t&&K_(n,e,t,i)}function ch(n,e){let t=n[$e];(t&3)===e&&(t&=16383,t+=1,n[$e]=t)}function K_(n,e,t,i){let r=i!==void 0?n[Fs]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[Fs]+=65536),(a<s||s==-1)&&(yT(n,t,e,c),n[Fs]=(n[Fs]&4294901760)+c+2),c++}function Iy(n,e){ti(4,n,e);let t=gt(null);try{e.call(n)}finally{gt(t),ti(5,n,e)}}function yT(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[$e]>>14<n[Fs]>>16&&(n[$e]&3)===e&&(n[$e]+=16384,Iy(a,s)):Iy(a,s)}var zs=-1,aa=class{factory;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i){this.factory=e,this.canSeeViewProviders=t,this.injectImpl=i}};function _T(n){return n instanceof aa}function xT(n){return(n.flags&8)!==0}function ET(n){return(n.flags&16)!==0}function MT(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];wT(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function ST(n){return n===3||n===4||n===6}function wT(n){return n.charCodeAt(0)===64}function J_(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?Ry(n,t,r,null,e[++i]):Ry(n,t,r,null,null))}}return n}function Ry(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){if(i===null){r!==null&&(n[s+1]=r);return}else if(i===n[s+1]){n[s+2]=r;return}}s++,i!==null&&s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),i!==null&&n.splice(s++,0,i),r!==null&&n.splice(s++,0,r)}var lh={},Hs=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){i=Bl(i);let r=this.injector.get(e,lh,i);return r!==lh||t===lh?r:this.parentInjector.get(e,t,i)}};function Q_(n){return n!==zs}function Sl(n){return n&32767}function bT(n){return n>>16}function wl(n,e){let t=bT(n),i=e;for(;t>0;)i=i[ua],t--;return i}var Th=!0;function Ny(n){let e=Th;return Th=n,e}var TT=256,e0=TT-1,t0=5,CT=0,ni={};function DT(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(ta)&&(i=t[ta]),i==null&&(i=t[ta]=CT++);let r=i&e0,s=1<<r;e.data[n+(r>>t0)]|=s}function n0(n,e){let t=i0(n,e);if(t!==-1)return t;let i=e[qe];i.firstCreatePass&&(n.injectorIndex=e.length,uh(i.data,n),uh(e,null),uh(i.blueprint,null));let r=up(n,e),s=n.injectorIndex;if(Q_(r)){let o=Sl(r),a=wl(r,e),c=a[qe].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function uh(n,e){n.push(0,0,0,0,0,0,0,0,e)}function i0(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function up(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=c0(r),i===null)return zs;if(t++,r=r[ua],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return zs}function AT(n,e,t){DT(n,e,t)}function r0(n,e,t){if(t&Ke.Optional||n!==void 0)return n;Zh(e,"NodeInjector")}function s0(n,e,t,i){if(t&Ke.Optional&&i===void 0&&(i=null),!(t&(Ke.Self|Ke.Host))){let r=n[tr],s=Pn(void 0);try{return r?r.get(e,i,t&Ke.Optional):x_(e,i,t&Ke.Optional)}finally{Pn(s)}}return r0(i,e,t)}function o0(n,e,t,i=Ke.Default,r){if(n!==null){if(e[$e]&2048&&!(i&Ke.Self)){let o=OT(n,e,t,i,ni);if(o!==ni)return o}let s=a0(n,e,t,i,ni);if(s!==ni)return s}return s0(e,t,i,r)}function a0(n,e,t,i,r){let s=NT(t);if(typeof s=="function"){if(!j_(e,n,i))return i&Ke.Host?r0(r,t,i):s0(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&Ke.Optional))Zh(t);else return o}finally{X_()}}else if(typeof s=="number"){let o=null,a=i0(n,e),c=zs,l=i&Ke.Host?e[Ln][Xn]:null;for((a===-1||i&Ke.SkipSelf)&&(c=a===-1?up(n,e):e[a+8],c===zs||!Oy(i,!1)?a=-1:(o=e[qe],a=Sl(c),e=wl(c,e)));a!==-1;){let u=e[qe];if(Py(s,a,u.data)){let d=IT(a,e,t,o,i,l);if(d!==ni)return d}c=e[a+8],c!==zs&&Oy(i,e[qe].data[a+8]===l)&&Py(s,a,e)?(o=u,a=Sl(c),e=wl(c,e)):a=-1}}return r}function IT(n,e,t,i,r,s){let o=e[qe],a=o.data[n+8],c=i==null?Js(a)&&Th:i!=o&&(a.type&3)!==0,l=r&Ke.Host&&s===a,u=RT(a,o,t,c,l);return u!==null?Ch(e,o,u,a):ni}function RT(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,d=i?a:a+u,f=r?a+u:l;for(let h=d;h<f;h++){let g=o[h];if(h<c&&t===g||h>=c&&g.type===t)return h}if(r){let h=o[c];if(h&&Qs(h)&&h.type===t)return c}return null}function Ch(n,e,t,i){let r=n[t],s=e.data;if(_T(r)){let o=r;o.resolving&&Sb(Mb(s[t]));let a=Ny(o.canSeeViewProviders);o.resolving=!0;let c,l=o.injectImpl?Pn(o.injectImpl):null,u=j_(n,i,Ke.Default);try{r=n[t]=o.factory(void 0,s,n,i),e.firstCreatePass&&t>=i.directiveStart&&gT(t,s[t],e)}finally{l!==null&&Pn(l),Ny(a),o.resolving=!1,X_()}}return r}function NT(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(ta)?n[ta]:void 0;return typeof e=="number"?e>=0?e&e0:PT:e}function Py(n,e,t){let i=1<<n;return!!(t[e+(n>>t0)]&i)}function Oy(n,e){return!(n&Ke.Self)&&!(n&Ke.Host&&e)}var Pr=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return o0(this._tNode,this._lView,e,Bl(i),t)}};function PT(){return new Pr(Vr(),Ti())}function dp(n){return Yh(()=>{let e=n.prototype.constructor,t=e[gl]||Dh(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let s=r[gl]||Dh(r);if(s&&s!==t)return s;r=Object.getPrototypeOf(r)}return s=>new s})}function Dh(n){return p_(n)?()=>{let e=Dh(On(n));return e&&e()}:js(n)}function OT(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[$e]&2048&&!oa(o);){let a=a0(s,o,t,i|Ke.Self,ni);if(a!==ni)return a;let c=s.parent;if(!c){let l=o[A_];if(l){let u=l.get(t,ni,i);if(u!==ni)return u}c=c0(o),o=o[ua]}s=c}return r}function c0(n){let e=n[qe],t=e.type;return t===2?e.declTNode:t===1?n[Xn]:null}function Ly(n,e=null,t=null,i){let r=l0(n,e,t,i);return r.resolveInjectorInitializers(),r}function l0(n,e=null,t=null,i,r=new Set){let s=[t||Ws,Ob(n)];return i=i||(typeof n=="object"?void 0:gn(n)),new ra(s,e||np(),i||null,r)}var En=class n{static THROW_IF_NOT_FOUND=na;static NULL=new _l;static create(e,t){if(Array.isArray(e))return Ly({name:""},t,e,"");{let i=e.name??"";return Ly({name:i},e.parent,e.providers,i)}}static \u0275prov=ke({token:n,providedIn:"any",factory:()=>et(M_)});static __NG_ELEMENT_ID__=-1};var LT=new Ae("");LT.__NG_ELEMENT_ID__=n=>{let e=Vr();if(e===null)throw new Pe(204,!1);if(e.type&2)return e.value;if(n&Ke.Optional)return null;throw new Pe(204,!1)};var u0=!1,fa=(()=>{class n{static __NG_ELEMENT_ID__=FT;static __NG_ENV_ID__=t=>t}return n})(),Ah=class extends fa{_lView;constructor(e){super(),this._lView=e}onDestroy(e){return k_(this._lView,e),()=>eT(this._lView,e)}};function FT(){return new Ah(Ti())}var Xs=class{},jl=new Ae("",{providedIn:"root",factory:()=>!1});var d0=new Ae(""),f0=new Ae(""),Ai=(()=>{class n{taskId=0;pendingTasks=new Set;get _hasPendingTasks(){return this.hasPendingTasks.value}hasPendingTasks=new Wt(!1);add(){this._hasPendingTasks||this.hasPendingTasks.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.pendingTasks.size===0&&this._hasPendingTasks&&this.hasPendingTasks.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this._hasPendingTasks&&this.hasPendingTasks.next(!1)}static \u0275prov=ke({token:n,providedIn:"root",factory:()=>new n})}return n})();var Ih=class extends Yt{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,D_()&&(this.destroyRef=Q(fa,{optional:!0})??void 0,this.pendingTasks=Q(Ai,{optional:!0})??void 0)}emit(e){let t=gt(null);try{super.next(e)}finally{gt(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof Rt&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{e(t),i!==void 0&&this.pendingTasks?.remove(i)})}}},mn=Ih;function bl(...n){}function h0(n){let e,t;function i(){n=bl;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function Fy(n){return queueMicrotask(()=>n()),()=>{n=bl}}var fp="isAngularZone",Tl=fp+"_ID",kT=0,kt=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new mn(!1);onMicrotaskEmpty=new mn(!1);onStable=new mn(!1);onError=new mn(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=u0}=e;if(typeof Zone>"u")throw new Pe(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,VT(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(fp)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new Pe(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new Pe(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,UT,bl,bl);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},UT={};function hp(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function BT(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){h0(()=>{n.callbackScheduled=!1,Rh(n),n.isCheckStableRunning=!0,hp(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Rh(n)}function VT(n){let e=()=>{BT(n)},t=kT++;n._inner=n._inner.fork({name:"angular",properties:{[fp]:!0,[Tl]:t,[Tl+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(zT(c))return i.invokeTask(s,o,a,c);try{return ky(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),Uy(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return ky(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!HT(c)&&e(),Uy(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Rh(n),hp(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Rh(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function ky(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function Uy(n){n._nesting--,hp(n)}var Nh=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new mn;onMicrotaskEmpty=new mn;onStable=new mn;onError=new mn;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function zT(n){return p0(n,"__ignore_ng_zone__")}function HT(n){return p0(n,"__scheduler_tick__")}function p0(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var Ci=class{_console=console;handleError(e){this._console.error("ERROR",e)}},GT=new Ae("",{providedIn:"root",factory:()=>{let n=Q(kt),e=Q(Ci);return t=>n.runOutsideAngular(()=>e.handleError(t))}});function By(n,e){return f_(n,e)}function jT(n){return f_(d_,n)}var m0=(By.required=jT,By);function WT(){return pp(Vr(),Ti())}function pp(n,e){return new to(Di(n,e))}var to=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=WT}return n})();var Vy=new Set;function or(n){Vy.has(n)||(Vy.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var $T="ngSkipHydration",qT="ngskiphydration";function g0(n){let e=n.mergedAttrs;if(e===null)return!1;for(let t=0;t<e.length;t+=2){let i=e[t];if(typeof i=="number")return!1;if(typeof i=="string"&&i.toLowerCase()===qT)return!0}return!1}function v0(n){return n.hasAttribute($T)}function Cl(n){return(n.flags&128)===128}function XT(n){if(Cl(n))return!0;let e=n.parent;for(;e;){if(Cl(n)||g0(e))return!0;e=e.parent}return!1}var y0=function(n){return n[n.OnPush=0]="OnPush",n[n.Default=1]="Default",n}(y0||{}),_0=new Map,YT=0;function ZT(){return YT++}function KT(n){_0.set(n[zl],n)}function Ph(n){_0.delete(n[zl])}var zy="__ngContext__";function ha(n,e){ii(e)?(n[zy]=e[zl],KT(e)):n[zy]=e}function x0(n){return M0(n[sa])}function E0(n){return M0(n[qn])}function M0(n){for(;n!==null&&!oi(n);)n=n[qn];return n}var Oh;function S0(n){Oh=n}function Wl(){if(Oh!==void 0)return Oh;if(typeof document<"u")return document;throw new Pe(210,!1)}var Ur=new Ae("",{providedIn:"root",factory:()=>JT}),JT="ng",mp=new Ae(""),Ii=new Ae("",{providedIn:"platform",factory:()=>"unknown"});var gp=new Ae("",{providedIn:"root",factory:()=>Wl().body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});function QT(){let n=new zr;return Q(Ii)==="browser"&&(n.store=eC(Wl(),Q(Ur))),n}var zr=(()=>{class n{static \u0275prov=ke({token:n,providedIn:"root",factory:QT});store={};onSerializeCallbacks={};get(t,i){return this.store[t]!==void 0?this.store[t]:i}set(t,i){this.store[t]=i}remove(t){delete this.store[t]}hasKey(t){return this.store.hasOwnProperty(t)}get isEmpty(){return Object.keys(this.store).length===0}onSerialize(t,i){this.onSerializeCallbacks[t]=i}toJson(){for(let t in this.onSerializeCallbacks)if(this.onSerializeCallbacks.hasOwnProperty(t))try{this.store[t]=this.onSerializeCallbacks[t]()}catch(i){console.warn("Exception in onSerialize callback: ",i)}return JSON.stringify(this.store).replace(/</g,"\\u003C")}}return n})();function eC(n,e){let t=n.getElementById(e+"-state");if(t?.textContent)try{return JSON.parse(t.textContent)}catch(i){console.warn("Exception while restoring TransferState for app "+e,i)}return{}}var w0="h",b0="b",tC="f",nC="n",iC="e",rC="t",vp="c",T0="x",Dl="r",sC="i",oC="n",C0="d";var aC="di",cC="s",lC="p";var al=new Ae(""),D0=!1,A0=new Ae("",{providedIn:"root",factory:()=>D0});var I0=new Ae(""),uC=!1,dC=new Ae(""),Hy=new Ae("",{providedIn:"root",factory:()=>new Map}),yp=function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n}(yp||{}),no=new Ae("");var ks=function(n){return n[n.EarlyRead=0]="EarlyRead",n[n.Write=1]="Write",n[n.MixedReadWrite=2]="MixedReadWrite",n[n.Read=3]="Read",n}(ks||{}),R0=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=ke({token:n,providedIn:"root",factory:()=>new n})}return n})(),fC=[ks.EarlyRead,ks.Write,ks.MixedReadWrite,ks.Read],hC=(()=>{class n{ngZone=Q(kt);scheduler=Q(Xs);errorHandler=Q(Ci,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){Q(no,{optional:!0})}execute(){this.executing=!0;for(let t of fC)for(let i of this.sequences)if(!(i.erroredOrDestroyed||!i.hooks[t]))try{i.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>i.hooks[t](i.pipelinedValue),i.snapshot))}catch(r){i.erroredOrDestroyed=!0,this.errorHandler?.handleError(r)}this.executing=!1;for(let t of this.sequences)t.afterRun(),t.once&&(this.sequences.delete(t),t.destroy());for(let t of this.deferredRegistrations)this.sequences.add(t);this.deferredRegistrations.size>0&&this.scheduler.notify(8),this.deferredRegistrations.clear()}register(t){this.executing?this.deferredRegistrations.add(t):(this.sequences.add(t),this.scheduler.notify(7))}unregister(t){this.executing&&this.sequences.has(t)?(t.erroredOrDestroyed=!0,t.pipelinedValue=void 0,t.once=!0):(this.sequences.delete(t),this.deferredRegistrations.delete(t))}maybeTrace(t,i){return i?i.run(yp.AFTER_NEXT_RENDER,t):t()}static \u0275prov=ke({token:n,providedIn:"root",factory:()=>new n})}return n})(),Lh=class{impl;hooks;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(e,t,i,r,s=null){this.impl=e,this.hooks=t,this.once=i,this.snapshot=s,this.unregisterOnDestroy=r?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.()}};function _p(n,e){!e?.injector&&Wb(_p);let t=e?.injector??Q(En);return or("NgAfterNextRender"),mC(n,t,e,!0)}function pC(n,e){if(n instanceof Function){let t=[void 0,void 0,void 0,void 0];return t[e]=n,t}else return[n.earlyRead,n.write,n.mixedReadWrite,n.read]}function mC(n,e,t,i){let r=e.get(R0);r.impl??=e.get(hC);let s=e.get(no,null,{optional:!0}),o=t?.phase??ks.MixedReadWrite,a=t?.manualCleanup!==!0?e.get(fa):null,c=new Lh(r.impl,pC(n,o),i,a,s?.snapshot(null));return r.impl.register(c),c}var pn=function(n){return n[n.NOT_STARTED=0]="NOT_STARTED",n[n.IN_PROGRESS=1]="IN_PROGRESS",n[n.COMPLETE=2]="COMPLETE",n[n.FAILED=3]="FAILED",n}(pn||{}),Gy=0,gC=1,zt=function(n){return n[n.Placeholder=0]="Placeholder",n[n.Loading=1]="Loading",n[n.Complete=2]="Complete",n[n.Error=3]="Error",n}(zt||{});var vC=0,xp=1;var yC=4,_C=5;var xC=7,Gs=8,EC=9,N0=function(n){return n[n.Manual=0]="Manual",n[n.Playthrough=1]="Playthrough",n}(N0||{});function fl(n,e){let t=SC(n),i=e[t];if(i!==null){for(let r of i)r();e[t]=null}}function MC(n){fl(1,n),fl(0,n),fl(2,n)}function SC(n){let e=yC;return n===1?e=_C:n===2&&(e=EC),e}function P0(n){return n+1}function $l(n,e){let t=n[qe],i=P0(e.index);return n[i]}function ql(n,e){let t=P0(e.index);return n.data[t]}function wC(n,e,t){let i=e[qe],r=ql(i,t);switch(n){case zt.Complete:return r.primaryTmplIndex;case zt.Loading:return r.loadingTmplIndex;case zt.Error:return r.errorTmplIndex;case zt.Placeholder:return r.placeholderTmplIndex;default:return null}}function jy(n,e){return e===zt.Placeholder?n.placeholderBlockConfig?.[Gy]??null:e===zt.Loading?n.loadingBlockConfig?.[Gy]??null:null}function bC(n){return n.loadingBlockConfig?.[gC]??null}function Wy(n,e){if(!n||n.length===0)return e;let t=new Set(n);for(let i of e)t.add(i);return n.length===t.size?n:Array.from(t)}function TC(n,e){let t=e.primaryTmplIndex+nn;return rp(n,t)}var Xl="ngb";var CC=(n,e,t)=>{let i=n,r=i.__jsaction_fns??new Map,s=r.get(e)??[];s.push(t),r.set(e,s),i.__jsaction_fns=r},DC=(n,e)=>{let t=n.getAttribute(Xl)??"",i=n,r=e.get(t)??new Set;r.has(i)||r.add(i),e.set(t,r)};var AC=n=>{n.removeAttribute(eh.JSACTION),n.removeAttribute(Xl),n.__jsaction_fns=void 0},IC=new Ae("",{providedIn:"root",factory:()=>({})});function O0(n,e){let t=e?.__jsaction_fns?.get(n.type);if(t)for(let i of t)i(n)}var L0=new Ae("");var RC="__nghData__",F0=RC,NC="__nghDeferData__",PC=NC,dh="ngh",OC="nghm",k0=()=>null;function LC(n,e,t=!1){let i=n.getAttribute(dh);if(i==null)return null;let[r,s]=i.split("|");if(i=t?s:r,!i)return null;let o=s?`|${s}`:"",a=t?r:o,c={};if(i!==""){let u=e.get(zr,null,{optional:!0});u!==null&&(c=u.get(F0,[])[Number(i)])}let l={data:c,firstChild:n.firstChild??null};return t&&(l.firstChild=n,Yl(l,0,n.nextSibling)),a?n.setAttribute(dh,a):n.removeAttribute(dh),l}function FC(){k0=LC}function U0(n,e,t=!1){return k0(n,e,t)}function kC(n){let e=n._lView;return e[qe].type===2?null:(oa(e)&&(e=e[nn]),e)}function UC(n){return n.textContent?.replace(/\s/gm,"")}function BC(n){let e=Wl(),t=e.createNodeIterator(n,NodeFilter.SHOW_COMMENT,{acceptNode(s){let o=UC(s);return o==="ngetn"||o==="ngtns"?NodeFilter.FILTER_ACCEPT:NodeFilter.FILTER_REJECT}}),i,r=[];for(;i=t.nextNode();)r.push(i);for(let s of r)s.textContent==="ngetn"?s.replaceWith(e.createTextNode("")):s.remove()}function Yl(n,e,t){n.segmentHeads??={},n.segmentHeads[e]=t}function Fh(n,e){return n.segmentHeads?.[e]??null}function VC(n){return n.get(dC,!1,{optional:!0})}function zC(n,e){let t=n.data,i=t[iC]?.[e]??null;return i===null&&t[vp]?.[e]&&(i=Ep(n,e)),i}function B0(n,e){return n.data[vp]?.[e]??null}function Ep(n,e){let t=B0(n,e)??[],i=0;for(let r of t)i+=r[Dl]*(r[T0]??1);return i}function HC(n){if(typeof n.disconnectedNodes>"u"){let e=n.data[C0];n.disconnectedNodes=e?new Set(e):null}return n.disconnectedNodes}function pa(n,e){if(typeof n.disconnectedNodes>"u"){let t=n.data[C0];n.disconnectedNodes=t?new Set(t):null}return!!HC(n)?.has(e)}function GC(n,e){let t=e.get(L0),r=e.get(zr).get(PC,{}),s=!1,o=n,a=null,c=[];for(;!s&&o;){s=t.has(o);let l=t.hydrating.get(o);if(a===null&&l!=null){a=l.promise;break}c.unshift(o),o=r[o][lC]}return{parentBlockPromise:a,hydrationQueue:c}}function V0(n,e){let t=n.contentQueries;if(t!==null){let i=gt(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];G_(s),a.contentQueries(2,e[o],o)}}}finally{gt(i)}}}function kh(n,e,t){G_(0);let i=gt(null);try{e(n,t)}finally{gt(i)}}function z0(n,e,t){if(R_(e)){let i=gt(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{gt(i)}}}var si=function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n}(si||{});var jC=/^>|^->|<!--|-->|--!>|<!-$/g,WC=/(<|>)/g,$C="\u200B$1\u200B";function qC(n){return n.replace(jC,e=>e.replace(WC,$C))}function XC(n){return n.ownerDocument.body}function H0(n){return n instanceof Function?n():n}function YC(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var G0="ng-template";function ZC(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&YC(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(Mp(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function Mp(n){return n.type===4&&n.value!==G0}function KC(n,e,t){let i=n.type===4&&!t?G0:n.value;return e===i}function JC(n,e,t){let i=4,r=n.attrs,s=r!==null?tD(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!$n(i)&&!$n(c))return!1;if(o&&$n(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!KC(n,c,t)||c===""&&e.length===1){if($n(i))return!1;o=!0}}else if(i&8){if(r===null||!ZC(n,r,c,t)){if($n(i))return!1;o=!0}}else{let l=e[++a],u=QC(c,r,Mp(n),t);if(u===-1){if($n(i))return!1;o=!0;continue}if(l!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&l!==d){if($n(i))return!1;o=!0}}}}return $n(i)||o}function $n(n){return(n&1)===0}function QC(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return nD(e,n)}function eD(n,e,t=!1){for(let i=0;i<e.length;i++)if(JC(n,e[i],t))return!0;return!1}function tD(n){for(let e=0;e<n.length;e++){let t=n[e];if(ST(t))return e}return n.length}function nD(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function $y(n,e){return n?":not("+e.trim()+")":e}function iD(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!$n(o)&&(e+=$y(s,r),r=""),i=o,s=s||!$n(i);t++}return r!==""&&(e+=$y(s,r)),e}function rD(n){return n.map(iD).join(",")}function sD(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!$n(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var j0={};function W0(n,e){return n.createText(e)}function $0(n,e){return n.createComment(qC(e))}function Sp(n,e,t){return n.createElement(e,t)}function Al(n,e,t,i,r){n.insertBefore(e,t,i,r)}function q0(n,e,t){n.appendChild(e,t)}function qy(n,e,t,i,r){i!==null?Al(n,e,t,i,r):q0(n,e,t)}function wp(n,e,t){n.removeChild(null,e,t)}function X0(n){n.textContent=""}function oD(n,e,t){n.setAttribute(e,"style",t)}function aD(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function Y0(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&MT(n,e,i),r!==null&&aD(n,e,r),s!==null&&oD(n,e,s)}function cD(n,e,t,i){if(!i)if((e[$e]&3)===3){let s=n.preOrderCheckHooks;s!==null&&ul(e,s,t)}else{let s=n.preOrderHooks;s!==null&&dl(e,s,0,t)}kr(t)}var ir=function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n}(ir||{});function Z0(n,e,t,i,r,s){let o=gt(null);try{let a=null;r&ir.SignalBased&&(a=e[i][Ss]),a!==null&&a.transformFn!==void 0&&(s=a.transformFn(s)),r&ir.HasDecoratorInputTransform&&(s=n.inputTransforms[i].call(e,s)),n.setInput!==null?n.setInput(e,a,s,t,i):N_(e,a,i,s)}finally{gt(o)}}function bp(n,e,t,i,r,s,o,a,c,l,u){let d=e.blueprint.slice();return d[xn]=r,d[$e]=i|4|128|8|64|1024,(l!==null||n&&n[$e]&2048)&&(d[$e]|=2048),F_(d),d[$t]=d[ua]=n,d[Si]=t,d[nr]=o||n&&n[nr],d[Zt]=a||n&&n[Zt],d[tr]=c||n&&n[tr]||null,d[Xn]=s,d[zl]=ZT(),d[ri]=u,d[A_]=l,d[Ln]=e.type==2?n[Ln]:d,d}function K0(n,e,t,i,r){let s=pT(),o=i&2;try{kr(-1),o&&e.length>nn&&cD(n,e,nn,!1),ti(o?2:0,r),t(i,r)}finally{kr(s),ti(o?3:1,r)}}function lD(n,e,t){U_()&&(ha(Di(t,e),e),J0(n,e,t))}function J0(n,e,t){vD(n,e,t),(t.flags&64)===64&&yD(n,e,t)}function uD(n,e,t=Di){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function dD(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Q0(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function Q0(n,e,t,i,r,s,o,a,c,l,u){let d=nn+i,f=d+r,h=fD(d,f),g=typeof l=="function"?l():l;return h[qe]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:f,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:u}}function fD(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:j0);return t}function hD(n,e,t,i){let s=i.get(A0,D0)||t===si.ShadowDom,o=n.selectRootElement(e,s);return pD(o),o}function pD(n){ex(n)}var ex=()=>null;function mD(n){v0(n)?X0(n):BC(n)}function gD(){ex=mD}function vD(n,e,t){let i=t.directiveStart,r=t.directiveEnd;Js(t)&&ED(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||n0(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=Ch(e,n,o,t);if(ha(c,e),s!==null&&MD(e,o-i,c,a,t,s),Qs(a)){let l=Lr(t.index,e);l[Si]=Ch(e,n,o,t)}}}function yD(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=fT();try{kr(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];bh(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&_D(c,l)}}finally{kr(-1),bh(o)}}function _D(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function xD(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];eD(e,s.selectors,!1)&&(i??=[],Qs(s)?i.unshift(s):i.push(s))}return i}function tx(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function ED(n,e,t){let i=Di(e,n),r=dD(t),s=n[nr].rendererFactory,o=nx(n,bp(n,r,null,tx(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function MD(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;){let c=o[a++],l=o[a++],u=o[a++],d=o[a++];Z0(i,t,c,l,u,d)}}function SD(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function nx(n,e){return n[sa]?n[Ty][qn]=e:n[sa]=e,n[Ty]=e,e}function Tp(n,e){let t=n[tr],i=t?t.get(Ci,null):null;i&&i.handleError(e)}function ix(n,e,t,i,r){for(let s=0;s<t.length;){let o=t[s++],a=t[s++],c=t[s++],l=e[o],u=n.data[o];Z0(u,l,i,a,c,r)}}function wD(n,e){let t=Lr(e,n),i=t[qe];bD(i,t);let r=t[xn];r!==null&&t[ri]===null&&(t[ri]=U0(r,t[tr])),Cp(i,t,t[Si])}function bD(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Cp(n,e,t){cp(e);try{let i=n.viewQuery;i!==null&&kh(1,i,t);let r=n.template;r!==null&&K0(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[qs]?.finishViewCreation(n),n.staticContentQueries&&V0(n,e),n.staticViewQueries&&kh(2,n.viewQuery,t);let s=n.components;s!==null&&TD(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[$e]&=-5,lp()}}function TD(n,e){for(let t=0;t<e.length;t++)wD(n,e[t])}var Hr=function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n}(Hr||{}),CD;function Dp(n,e){return CD(n,e)}function io(n){return(n.flags&32)===32}function Us(n,e,t,i,r){if(i!=null){let s,o=!1;oi(i)?s=i:ii(i)&&(o=!0,i=i[xn]);let a=bi(i);n===0&&t!==null?r==null?q0(e,t,a):Al(e,t,a,r||null,!0):n===1&&t!==null?Al(e,t,a,r||null,!0):n===2?wp(e,a,o):n===3&&e.destroyNode(a),s!=null&&zD(e,n,s,t,r)}}function DD(n,e){rx(n,e),e[xn]=null,e[Xn]=null}function AD(n,e,t,i,r,s){i[xn]=r,i[Xn]=e,Zl(n,i,t,1,r,s)}function rx(n,e){e[nr].changeDetectionScheduler?.notify(10),Zl(n,e,e[Zt],2,null,null)}function ID(n){let e=n[sa];if(!e)return fh(n[qe],n);for(;e;){let t=null;if(ii(e))t=e[sa];else{let i=e[yn];i&&(t=i)}if(!t){for(;e&&!e[qn]&&e!==n;)ii(e)&&fh(e[qe],e),e=e[$t];e===null&&(e=n),ii(e)&&fh(e[qe],e),t=e&&e[qn]}e=t}}function RD(n,e,t,i){let r=yn+i,s=t.length;i>0&&(t[r-1][qn]=e),i<s-yn?(e[qn]=t[r],E_(t,yn+i,e)):(t.push(e),e[qn]=null),e[$t]=t;let o=e[$s];o!==null&&t!==o&&sx(o,e);let a=e[qs];a!==null&&a.insertView(n),Sh(e),e[$e]|=128}function sx(n,e){let t=n[Ml],i=e[$t];if(ii(i))n[$e]|=2;else{let r=i[$t][Ln];e[Ln]!==r&&(n[$e]|=2)}t===null?n[Ml]=[e]:t.push(e)}function Ap(n,e){let t=n[Ml],i=t.indexOf(e);t.splice(i,1)}function Il(n,e){if(n.length<=yn)return;let t=yn+e,i=n[t];if(i){let r=i[$s];r!==null&&r!==n&&Ap(r,i),e>0&&(n[t-1][qn]=i[qn]);let s=yl(n,yn+e);DD(i[qe],i);let o=s[qs];o!==null&&o.detachView(s[qe]),i[$t]=null,i[qn]=null,i[$e]&=-129}return i}function Ip(n,e){if(eo(e))return;let t=e[Zt];t.destroyNode&&Zl(n,e,t,3,null,null),ID(e)}function fh(n,e){if(eo(e))return;let t=gt(null);try{e[$e]&=-129,e[$e]|=256,e[vn]&&Of(e[vn]),PD(n,e),ND(n,e),e[qe].type===1&&e[Zt].destroy();let i=e[$s];if(i!==null&&oi(e[$t])){i!==e[$t]&&Ap(i,e);let r=e[qs];r!==null&&r.detachView(n)}Ph(e)}finally{gt(t)}}function ND(n,e){let t=n.cleanup,i=e[by];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[by]=null);let r=e[Ji];if(r!==null){e[Ji]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[xl];if(s!==null){e[xl]=null;for(let o of s)o.destroy()}}function PD(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof aa)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];ti(4,a,c);try{c.call(a)}finally{ti(5,a,c)}}else{ti(4,r,s);try{s.call(r)}finally{ti(5,r,s)}}}}}function OD(n,e,t){return LD(n,e.parent,t)}function LD(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[xn];if(Js(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===si.None||r===si.Emulated)return null}return Di(i,t)}function FD(n,e,t){return UD(n,e,t)}function kD(n,e,t){return n.type&40?Di(n,t):null}var UD=kD,Xy;function BD(n,e,t,i){let r=OD(n,i,e),s=e[Zt],o=i.parent||e[Xn],a=FD(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)qy(s,r,t[c],a,!1);else qy(s,r,t,a,!1);Xy!==void 0&&Xy(s,i,e,t,r)}function ea(n,e){if(e!==null){let t=e.type;if(t&3)return Di(e,n);if(t&4)return Uh(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return ea(n,i);{let r=n[e.index];return oi(r)?Uh(-1,r):bi(r)}}else{if(t&128)return ea(n,e.next);if(t&32)return Dp(e,n)()||bi(n[e.index]);{let i=ox(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=Fr(n[Ln]);return ea(r,i)}else return ea(n,e.next)}}}return null}function ox(n,e){if(e!==null){let i=n[Ln][Xn],r=e.projection;return i.projection[r]}return null}function Uh(n,e){let t=yn+n+1;if(t<e.length){let i=e[t],r=i[qe].firstChild;if(r!==null)return ea(i,r)}return e[wi]}function Rp(n,e,t,i,r,s,o){for(;t!=null;){if(t.type===128){t=t.next;continue}let a=i[t.index],c=t.type;if(o&&e===0&&(a&&ha(bi(a),i),t.flags|=2),!io(t))if(c&8)Rp(n,e,t.child,i,r,s,!1),Us(e,n,r,a,s);else if(c&32){let l=Dp(t,i),u;for(;u=l();)Us(e,n,r,u,s);Us(e,n,r,a,s)}else c&16?VD(n,e,i,t,r,s):Us(e,n,r,a,s);t=o?t.projectionNext:t.next}}function Zl(n,e,t,i,r,s){Rp(t,i,n.firstChild,e,r,s,!1)}function VD(n,e,t,i,r,s){let o=t[Ln],c=o[Xn].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];Us(e,n,r,u,s)}else{let l=c,u=o[$t];Cl(i)&&(l.flags|=128),Rp(n,e,l,u,r,s,!0)}}function zD(n,e,t,i,r){let s=t[wi],o=bi(t);s!==o&&Us(e,n,i,s,r);for(let a=yn;a<t.length;a++){let c=t[a];Zl(c[qe],c,n,e,i,s)}}function HD(n,e,t,i){let r=gt(null);try{let s=e.tView,a=n[$e]&4096?4096:16,c=bp(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[$s]=l;let u=n[qs];return u!==null&&(c[qs]=u.createEmbeddedView(s)),Cp(s,c,t),c}finally{gt(r)}}function Bh(n,e){return!e||e.firstChild===null||Cl(n)}function ax(n,e,t,i=!0){let r=e[qe];if(RD(r,e,n,t),i){let o=Uh(t,n),a=e[Zt],c=a.parentNode(n[wi]);c!==null&&AD(r,n[Xn],a,e,c,o)}let s=e[ri];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function GD(n,e){let t=Il(n,e);return t!==void 0&&Ip(t[qe],t),t}function Rl(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(bi(s)),oi(s)&&jD(s,i);let o=t.type;if(o&8)Rl(n,e,t.child,i);else if(o&32){let a=Dp(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=ox(e,t);if(Array.isArray(a))i.push(...a);else{let c=Fr(e[Ln]);Rl(c[qe],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function jD(n,e){for(let t=yn;t<n.length;t++){let i=n[t],r=i[qe].firstChild;r!==null&&Rl(i[qe],i,r,e)}n[wi]!==n[xn]&&e.push(n[wi])}var cx=[];function WD(n){return n[vn]??$D(n)}function $D(n){let e=cx.pop()??Object.create(XD);return e.lView=n,e}function qD(n){n.lView[vn]!==n&&(n.lView=null,cx.push(n))}var XD=Mt(be({},Pc),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{Gl(n.lView)},consumerOnSignalRead(){this.lView[vn]=this}});function YD(n){let e=n[vn]??Object.create(ZD);return e.lView=n,e}var ZD=Mt(be({},Pc),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=Fr(n.lView);for(;e&&!lx(e[qe]);)e=Fr(e);e&&op(e)},consumerOnSignalRead(){this.lView[vn]=this}});function lx(n){return n.type!==2}function ux(n){if(n[xl]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[xl])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[$e]&8192)}}var KD=100;function dx(n,e=!0,t=0){let r=n[nr].rendererFactory,s=!1;s||r.begin?.();try{JD(n,t)}catch(o){throw e&&Tp(n,o),o}finally{s||r.end?.()}}function JD(n,e){let t=H_();try{Ay(!0),Vh(n,e);let i=0;for(;Hl(n);){if(i===KD)throw new Pe(103,!1);i++,Vh(n,1)}}finally{Ay(t)}}function QD(n,e,t,i){if(eo(e))return;let r=e[$e],s=!1,o=!1;cp(e);let a=!0,c=null,l=null;s||(lx(n)?(l=WD(e),c=Nf(l)):gv()===null?(a=!1,l=YD(e),c=Nf(l)):e[vn]&&(Of(e[vn]),e[vn]=null));try{F_(e),lT(n.bindingStartIndex),t!==null&&K0(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let h=n.preOrderCheckHooks;h!==null&&ul(e,h,null)}else{let h=n.preOrderHooks;h!==null&&dl(e,h,0,null),ch(e,0)}if(o||eA(e),ux(e),fx(e,0),n.contentQueries!==null&&V0(n,e),!s)if(u){let h=n.contentCheckHooks;h!==null&&ul(e,h)}else{let h=n.contentHooks;h!==null&&dl(e,h,1),ch(e,1)}nA(n,e);let d=n.components;d!==null&&px(e,d,0);let f=n.viewQuery;if(f!==null&&kh(2,f,i),!s)if(u){let h=n.viewCheckHooks;h!==null&&ul(e,h)}else{let h=n.viewHooks;h!==null&&dl(e,h,2),ch(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[ah]){for(let h of e[ah])h();e[ah]=null}s||(e[$e]&=-73)}catch(u){throw s||Gl(e),u}finally{l!==null&&(_v(l,c),a&&qD(l)),lp()}}function fx(n,e){for(let t=x0(n);t!==null;t=E0(t))for(let i=yn;i<t.length;i++){let r=t[i];hx(r,e)}}function eA(n){for(let e=x0(n);e!==null;e=E0(e)){if(!(e[$e]&2))continue;let t=e[Ml];for(let i=0;i<t.length;i++){let r=t[i];op(r)}}}function tA(n,e,t){let i=Lr(e,n);hx(i,t)}function hx(n,e){sp(n)&&Vh(n,e)}function Vh(n,e){let i=n[qe],r=n[$e],s=n[vn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&Pf(s)),o||=!1,s&&(s.dirty=!1),n[$e]&=-9217,o)QD(i,n,i.template,n[Si]);else if(r&8192){ux(n),fx(n,1);let a=i.components;a!==null&&px(n,a,1)}}function px(n,e,t){for(let i=0;i<e.length;i++)tA(n,e[i],t)}function nA(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)kr(~r);else{let s=r,o=t[++i],a=t[++i];dT(o,s);let c=e[s];ti(24,c),a(2,c),ti(25,c)}}}finally{kr(-1)}}function Np(n,e){let t=H_()?64:1088;for(n[nr].changeDetectionScheduler?.notify(e);n;){n[$e]|=t;let i=Fr(n);if(oa(n)&&!i)return n;n=i}return null}var Ys=class{_lView;_cdRefInjectingView;notifyErrorHandler;_appRef=null;_attachedToViewContainer=!1;get rootNodes(){let e=this._lView,t=e[qe];return Rl(t,e,t.firstChild,[])}constructor(e,t,i=!0){this._lView=e,this._cdRefInjectingView=t,this.notifyErrorHandler=i}get context(){return this._lView[Si]}get dirty(){return!!(this._lView[$e]&9280)||!!this._lView[vn]?.dirty}set context(e){this._lView[Si]=e}get destroyed(){return eo(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[$t];if(oi(e)){let t=e[El],i=t?t.indexOf(this):-1;i>-1&&(Il(e,i),yl(t,i))}this._attachedToViewContainer=!1}Ip(this._lView[qe],this._lView)}onDestroy(e){k_(this._lView,e)}markForCheck(){Np(this._cdRefInjectingView||this._lView,4)}markForRefresh(){op(this._cdRefInjectingView||this._lView)}detach(){this._lView[$e]&=-129}reattach(){Sh(this._lView),this._lView[$e]|=128}detectChanges(){this._lView[$e]|=1024,dx(this._lView,this.notifyErrorHandler)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new Pe(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=oa(this._lView),t=this._lView[$s];t!==null&&!e&&Ap(t,this._lView),rx(this._lView[qe],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new Pe(902,!1);this._appRef=e;let t=oa(this._lView),i=this._lView[$s];i!==null&&!t&&sx(i,this._lView),Sh(this._lView)}};function iA(n,e,t,i,r){let s=n.data[e];if(s===null)s=rA(n,e,t,i,r),uT()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=aT();s.injectorIndex=o===null?-1:o.injectorIndex}return ap(s,!0),s}function rA(n,e,t,i,r){let s=V_(),o=z_(),a=o?s:s&&s.parent,c=n.data[e]=oA(n,a,t,e,i,r);return sA(n,c,s,o),c}function sA(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function oA(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return da()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:s,mergedAttrs:null,localNames:null,initialInputs:void 0,inputs:null,outputs:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var aA=new RegExp(`^(\\d+)*(${b0}|${w0})*(.*)`);function cA(n){let e=n.match(aA),[t,i,r,s]=e,o=i?parseInt(i,10):r,a=[];for(let[c,l,u]of s.matchAll(/(f|n)(\d*)/g)){let d=parseInt(u,10)||1;a.push(l,d)}return[o,...a]}function lA(n){return!n.prev&&n.parent?.type===8}function hh(n){return n.index-nn}function uA(n,e){let t=n.i18nNodes;if(t)return t.get(e)}function Kl(n,e,t,i){let r=hh(i),s=uA(n,r);if(s===void 0){let o=n.data[oC];if(o?.[r])s=fA(o[r],t);else if(e.firstChild===i)s=n.firstChild;else{let a=i.prev===null,c=i.prev??i.parent;if(lA(i)){let l=hh(i.parent);s=Fh(n,l)}else{let l=Di(c,t);if(a)s=l.firstChild;else{let u=hh(c),d=Fh(n,u);if(c.type===2&&d){let h=Ep(n,u)+1;s=Jl(h,d)}else s=l.nextSibling}}}}return s}function Jl(n,e){let t=e;for(let i=0;i<n;i++)t=t.nextSibling;return t}function dA(n,e){let t=n;for(let i=0;i<e.length;i+=2){let r=e[i],s=e[i+1];for(let o=0;o<s;o++)switch(r){case tC:t=t.firstChild;break;case nC:t=t.nextSibling;break}}return t}function fA(n,e){let[t,...i]=cA(n),r;if(t===w0)r=e[Ln][xn];else if(t===b0)r=XC(e[Ln][xn]);else{let s=Number(t);r=bi(e[s+nn])}return dA(r,i)}var hA=!1;function pA(n){hA=n}function mA(n){let e=n[ri];if(e){let{i18nNodes:t,dehydratedIcuData:i}=e;if(t&&i){let r=n[Zt];for(let s of i.values())gA(r,t,s)}e.i18nNodes=void 0,e.dehydratedIcuData=void 0}}function gA(n,e,t){for(let i of t.node.cases[t.case]){let r=e.get(i.index-nn);r&&wp(n,r,!1)}}function mx(n){let e=n[Or]??[],i=n[$t][Zt],r=[];for(let s of e)s.data[aC]!==void 0?r.push(s):vA(s,i);n[Or]=r}function vA(n,e){let t=0,i=n.firstChild;if(i){let r=n.data[Dl];for(;t<r;){let s=i.nextSibling;wp(e,i,!1),i=s,t++}}}function Pp(n){mx(n);let e=n[xn];ii(e)&&Nl(e);for(let t=yn;t<n.length;t++)Nl(n[t])}function Nl(n){mA(n);let e=n[qe];for(let t=nn;t<e.bindingStartIndex;t++)if(oi(n[t])){let i=n[t];Pp(i)}else ii(n[t])&&Nl(n[t])}function gx(n){let e=n._views;for(let t of e){let i=kC(t);i!==null&&i[xn]!==null&&(ii(i)?Nl(i):Pp(i))}}function yA(n,e,t,i){n!==null&&(t.cleanup(e),Pp(n.lContainer),gx(i))}function _A(n,e){let t=[];for(let i of e)for(let r=0;r<(i[T0]??1);r++){let s={data:i,firstChild:null};i[Dl]>0&&(s.firstChild=n,n=Jl(i[Dl],n)),t.push(s)}return[n,t]}var vx=()=>null;function xA(n,e){let t=n[Or];return!e||t===null||t.length===0?null:t[0].data[sC]===e?t.shift():(mx(n),null)}function EA(){vx=xA}function Yy(n,e){return vx(n,e)}var zh=class{},Pl=class{},Hh=class{resolveComponentFactory(e){throw Error(`No component factory found for ${gn(e)}.`)}},Zs=class{static NULL=new Hh},Ks=class{};var MA=(()=>{class n{static \u0275prov=ke({token:n,providedIn:"root",factory:()=>null})}return n})();function Zy(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=_y(r,a);else if(s==2){let c=a,l=e[++o];i=_y(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function ro(n,e=Ke.Default){let t=Ti();if(t===null)return et(n,e);let i=Vr();return o0(i,t,On(n),e)}function yx(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function SA(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let[a,c]=bA(n,t,o);CA(n,e,t,a,s,c)}s!==null&&i!==null&&wA(t,i,s)}function wA(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new Pe(-301,!1);i.push(e[r],s)}}function bA(n,e,t){let i=[],r=null;for(let s of t)s.findHostDirectiveDefs!==null&&(r??=new Map,s.findHostDirectiveDefs(s,i,r)),Qs(s)&&(i.push(s),TA(n,e,i.length-1));return Js(e)?i.push(...t.slice(1)):i.push(...t),[i,r]}function TA(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function CA(n,e,t,i,r,s){for(let l=0;l<i.length;l++)AT(n0(t,e),n,i[l].type);OA(t,n.data.length,i.length);for(let l=0;l<i.length;l++){let u=i[l];u.providersResolver&&u.providersResolver(u)}let o=!1,a=!1,c=yx(n,e,i.length,null);for(let l=0;l<i.length;l++){let u=i[l];t.mergedAttrs=J_(t.mergedAttrs,u.hostAttrs),IA(n,t,e,c,u),PA(c,u,r),u.contentQueries!==null&&(t.flags|=4),(u.hostBindings!==null||u.hostAttrs!==null||u.hostVars!==0)&&(t.flags|=64);let d=u.type.prototype;!o&&(d.ngOnChanges||d.ngOnInit||d.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),o=!0),!a&&(d.ngOnChanges||d.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),a=!0),c++}DA(n,t,s)}function DA(n,e,t){let i=e.directiveStart,r=e.directiveEnd,s=n.data,o=e.attrs,a=[],c=null,l=null;for(let u=i;u<r;u++){let d=s[u],f=t?t.get(d):null,h=f?f.inputs:null,g=f?f.outputs:null;c=Ky(0,d.inputs,u,c,h),l=Ky(1,d.outputs,u,l,g);let v=c!==null&&o!==null&&!Mp(e)?AA(c,u,o):null;a.push(v)}c!==null&&(c.hasOwnProperty("class")&&(e.flags|=8),c.hasOwnProperty("style")&&(e.flags|=16)),e.initialInputs=a,e.inputs=c,e.outputs=l}function Ky(n,e,t,i,r){for(let s in e){if(!e.hasOwnProperty(s))continue;let o=e[s];if(o===void 0)continue;i??={};let a,c=ir.None;Array.isArray(o)?(a=o[0],c=o[1]):a=o;let l=s;if(r!==null){if(!r.hasOwnProperty(s))continue;l=r[s]}n===0?Jy(i,t,l,a,c):Jy(i,t,l,a)}return i}function Jy(n,e,t,i,r){let s;n.hasOwnProperty(t)?(s=n[t]).push(e,i):s=n[t]=[e,i],r!==void 0&&s.push(r)}function AA(n,e,t){let i=null,r=0;for(;r<t.length;){let s=t[r];if(s===0){r+=4;continue}else if(s===5){r+=2;continue}if(typeof s=="number")break;if(n.hasOwnProperty(s)){i===null&&(i=[]);let o=n[s];for(let a=0;a<o.length;a+=3)if(o[a]===e){i.push(s,o[a+1],o[a+2],t[r+1]);break}}r+=2}return i}function IA(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=js(r.type,!0)),o=new aa(s,Qs(r),ro);n.blueprint[i]=o,t[i]=o,RA(n,e,i,yx(n,t,r.hostVars,j0),r)}function RA(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;NA(o)!=a&&o.push(a),o.push(t,i,s)}}function NA(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function PA(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;Qs(e)&&(t[""]=n)}}function OA(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function _x(n,e,t,i,r,s,o,a){let c=e.consts,l=Dy(c,o),u=iA(e,n,2,i,l);return s&&SA(e,t,u,Dy(c,a),r),u.mergedAttrs=J_(u.mergedAttrs,u.attrs),u.attrs!==null&&Zy(u,u.attrs,!1),u.mergedAttrs!==null&&Zy(u,u.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,u),u}function xx(n,e){vT(n,e),R_(e)&&n.queries.elementEnd(e)}var Ol=class extends Zs{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=er(e);return new ca(t,this.ngModule)}};function Qy(n,e){let t=[];for(let i in n){if(!n.hasOwnProperty(i))continue;let r=n[i];if(r===void 0)continue;let s=Array.isArray(r),o=s?r[0]:r,a=s?r[1]:ir.None;e?t.push({propName:o,templateName:i,isSignal:(a&ir.SignalBased)!==0}):t.push({propName:o,templateName:i})}return t}function LA(n,e,t){let i=e instanceof _n?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new Hs(t,i):t}function FA(n){let e=n.get(Ks,null);if(e===null)throw new Pe(407,!1);let t=n.get(MA,null),i=n.get(Xs,null);return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i}}function kA(n,e){let t=(n.selectors[0][0]||"div").toLowerCase();return Sp(e,t,t==="svg"?Kb:t==="math"?Jb:null)}var ca=class extends Pl{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;get inputs(){let e=this.componentDef,t=e.inputTransforms,i=Qy(e.inputs,!0);if(t!==null)for(let r of i)t.hasOwnProperty(r.propName)&&(r.transform=t[r.propName]);return i}get outputs(){return Qy(this.componentDef.outputs,!1)}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=rD(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r){let s=gt(null);try{let o=this.componentDef,a=i?["ng-version","19.1.5"]:sD(this.componentDef.selectors[0]),c=Q0(0,null,null,1,0,null,null,null,null,[a],null),l=LA(o,r||this.ngModule,e),u=FA(l),d=u.rendererFactory.createRenderer(null,o),f=i?hD(d,i,o.encapsulation,l):kA(o,d),h=bp(null,c,null,512|tx(o),null,null,u,d,l,null,U0(f,l,!0));h[nn]=f,cp(h);let g=null;try{let v=_x(nn,c,h,"#host",()=>[this.componentDef],!0,0);f&&(Y0(d,f,v),ha(f,h)),J0(c,h,v),z0(c,v,h),xx(c,v),t!==void 0&&UA(v,this.ngContentSelectors,t),g=Lr(v.index,h),h[Si]=g[Si],Cp(c,h,null)}catch(v){throw g!==null&&Ph(g),Ph(h),v}finally{lp()}return new Gh(this.componentType,h)}finally{gt(s)}}},Gh=class extends zh{_rootLView;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t){super(),this._rootLView=t,this._tNode=rp(t[qe],nn),this.location=pp(this._tNode,t),this.instance=Lr(this._tNode.index,t)[Si],this.hostView=this.changeDetectorRef=new Ys(t,void 0,!1),this.componentType=e}setInput(e,t){let i=this._tNode.inputs,r;if(i!==null&&(r=i[e])){if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let s=this._rootLView;ix(s[qe],s,r,e,t),this.previousInputValues.set(e,t);let o=Lr(this._tNode.index,s);Np(o,1)}}get injector(){return new Pr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function UA(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var Ql=(()=>{class n{static __NG_ELEMENT_ID__=BA}return n})();function BA(){let n=Vr();return zA(n,Ti())}var VA=Ql,Ex=class extends VA{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return pp(this._hostTNode,this._hostLView)}get injector(){return new Pr(this._hostTNode,this._hostLView)}get parentInjector(){let e=up(this._hostTNode,this._hostLView);if(Q_(e)){let t=wl(e,this._hostLView),i=Sl(e),r=t[qe].data[i+8];return new Pr(r,t)}else return new Pr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=e_(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-yn}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=Yy(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,Bh(this._hostTNode,o)),a}createComponent(e,t,i,r,s){let o=e&&!$b(e),a;if(o)a=t;else{let g=t||{};a=g.index,i=g.injector,r=g.projectableNodes,s=g.environmentInjector||g.ngModuleRef}let c=o?e:new ca(er(e)),l=i||this.parentInjector;if(!s&&c.ngModule==null){let v=(o?l:this.parentInjector).get(_n,null);v&&(s=v)}let u=er(c.componentType??{}),d=Yy(this._lContainer,u?.id??null),f=d?.firstChild??null,h=c.create(l,r,f,s);return this.insertImpl(h.hostView,a,Bh(this._hostTNode,d)),h}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(Qb(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[$t],l=new Ex(c,c[Xn],c[$t]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return ax(o,r,s,i),e.attachToViewContainerRef(),E_(ph(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=e_(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=Il(this._lContainer,t);i&&(yl(ph(this._lContainer),t),Ip(i[qe],i))}detach(e){let t=this._adjustIndex(e,-1),i=Il(this._lContainer,t);return i&&yl(ph(this._lContainer),t)!=null?new Ys(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function e_(n){return n[El]}function ph(n){return n[El]||(n[El]=[])}function zA(n,e){let t,i=e[n.index];return oi(i)?t=i:(t=SD(i,e,null,n),e[n.index]=t,nx(e,t)),Mx(t,e,n,i),new Ex(t,n,e)}function HA(n,e){let t=n[Zt],i=t.createComment(""),r=Di(e,n),s=t.parentNode(r);return Al(t,s,i,t.nextSibling(r),!1),i}var Mx=wx,Sx=()=>!1;function wx(n,e,t,i){if(n[wi])return;let r;t.type&8?r=bi(i):r=HA(e,t),n[wi]=r}function GA(n,e,t){if(n[wi]&&n[Or])return!0;let i=t[ri],r=e.index-nn;if(!i||XT(e)||pa(i,r))return!1;let o=Fh(i,r),a=i.data[vp]?.[r],[c,l]=_A(o,a);return n[wi]=c,n[Or]=l,!0}function jA(n,e,t,i){Sx(n,t,e)||wx(n,e,t,i)}function WA(){Mx=jA,Sx=GA}var rr=class{},la=class{};var jh=class extends rr{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Ol(this);constructor(e,t,i,r=!0){super(),this.ngModuleType=e,this._parent=t;let s=w_(e);this._bootstrapComponents=H0(s.bootstrap),this._r3Injector=l0(e,t,[{provide:rr,useValue:this},{provide:Zs,useValue:this.componentFactoryResolver},...i],gn(e),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let e=this._r3Injector;!e.destroyed&&e.destroy(),this.destroyCbs.forEach(t=>t()),this.destroyCbs=null}onDestroy(e){this.destroyCbs.push(e)}},Wh=class extends la{moduleType;constructor(e){super(),this.moduleType=e}create(e){return new jh(this.moduleType,e,[])}};var Ll=class extends rr{injector;componentFactoryResolver=new Ol(this);instance=null;constructor(e){super();let t=new ra([...e.providers,{provide:rr,useValue:this},{provide:Zs,useValue:this.componentFactoryResolver}],e.parent||np(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function eu(n,e,t=null){return new Ll({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var $A=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=ep(!1,t.type),r=i.length>0?eu([i],this._injector,`Standalone[${t.type.name}]`):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=ke({token:n,providedIn:"environment",factory:()=>new n(et(_n))})}return n})();function so(n){return Yh(()=>{let e=bx(n),t=Mt(be({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===y0.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get($A).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||si.Emulated,styles:n.styles||Ws,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&or("NgStandalone"),Tx(t);let i=n.dependencies;return t.directiveDefs=n_(i,!1),t.pipeDefs=n_(i,!0),t.id=YA(t),t})}function qA(n){return er(n)||Jh(n)}function XA(n){return n!==null}function t_(n,e){if(n==null)return ia;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a=ir.None;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s):(s=r,o=r),e?(t[s]=a!==ir.None?[i,a]:i,e[s]=o):t[s]=i}return t}function Op(n){return Yh(()=>{let e=bx(n);return Tx(e),e})}function bx(n){let e={};return{type:n.type,providersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputTransforms:null,inputConfig:n.inputs||ia,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||Ws,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,findHostDirectiveDefs:null,hostDirectives:null,inputs:t_(n.inputs,e),outputs:t_(n.outputs),debugInfo:null}}function Tx(n){n.features?.forEach(e=>e(n))}function n_(n,e){if(!n)return null;let t=e?Qh:qA;return()=>(typeof n=="function"?n():n).map(i=>t(i)).filter(XA)}function YA(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}var ZA=Cx;function Cx(n,e,t,i){return sr(!0),e[Zt].createComment("")}function KA(n,e,t,i){let r=e[ri],s=!r||da()||io(t)||pa(r,i);if(sr(s),s)return Cx(n,e,t,i);let o=r.data[rC]?.[i]??null;o!==null&&t.tView!==null&&t.tView.ssrId===null&&(t.tView.ssrId=o);let a=Kl(r,n,e,t);Yl(r,i,a);let c=Ep(r,i);return Jl(c,a)}function JA(){ZA=KA}var QA=(()=>{class n{cachedInjectors=new Map;getOrCreateInjector(t,i,r,s){if(!this.cachedInjectors.has(t)){let o=r.length>0?eu(r,i,s):null;this.cachedInjectors.set(t,o)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=ke({token:n,providedIn:"environment",factory:()=>new n})}return n})();var eI=new Ae("");function mh(n,e,t){return n.get(QA).getOrCreateInjector(e,n,t,"")}function tI(n,e,t){if(n instanceof Hs){let r=n.injector,s=n.parentInjector,o=mh(s,e,t);return new Hs(r,o)}let i=n.get(_n);if(i!==n){let r=mh(i,e,t);return new Hs(n,r)}return mh(n,e,t)}function Bs(n,e,t,i=!1){let r=t[$t],s=r[qe];if(eo(r))return;let o=$l(r,e),a=o[xp],c=o[xC];if(!(c!==null&&n<c)&&i_(a,n)&&i_(o[vC]??-1,n)){let l=ql(s,e),d=!i&&!0&&(bC(l)!==null||jy(l,zt.Loading)!==null||jy(l,zt.Placeholder))?rI:iI;try{d(n,o,t,e,r)}catch(f){Tp(r,f)}}}function nI(n,e){return n[Or]?.find(t=>t.data[cC]===e[xp])??null}function iI(n,e,t,i,r){let s=wC(n,r,i);if(s!==null){e[xp]=n;let o=r[qe],a=s+nn,c=rp(o,a),l=0;GD(t,l);let u;if(n===zt.Complete){let h=ql(o,i),g=h.providers;g&&g.length>0&&(u=tI(r[tr],h,g))}let d=nI(t,e);t[Or]=null;let f=HD(r,c,null,{injector:u,dehydratedView:d});if(ax(t,f,l,Bh(c,d)),Np(f,2),(n===zt.Complete||n===zt.Error)&&Array.isArray(e[Gs])){for(let h of e[Gs])h();e[Gs]=null}}}function i_(n,e){return n<e}function r_(n,e,t){n.loadingPromise.then(()=>{n.loadingState===pn.COMPLETE?Bs(zt.Complete,e,t):n.loadingState===pn.FAILED&&Bs(zt.Error,e,t)})}var rI=null;var Lp=(()=>{class n{log(t){console.log(t)}warn(t){console.warn(t)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"platform"})}return n})();var Dx=new Ae("");function ma(n){return!!n&&typeof n.then=="function"}function Ax(n){return!!n&&typeof n.subscribe=="function"}var Ix=new Ae("");var Rx=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=Q(Ix,{optional:!0})??[];injector=Q(En);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=Fn(this.injector,r);if(ma(s))t.push(s);else if(Ax(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),sI=(()=>{class n{static \u0275prov=ke({token:n,providedIn:"root",factory:()=>new $h})}return n})(),$h=class{queuedEffectCount=0;queues=new Map;schedule(e){this.enqueue(e)}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),this.queuedEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||(this.queuedEffectCount++,i.add(e))}flush(){for(;this.queuedEffectCount>0;)for(let[e,t]of this.queues)e===null?this.flushQueue(t):e.run(()=>this.flushQueue(t))}flushQueue(e){for(let t of e)e.delete(t),this.queuedEffectCount--,t.run()}},Gr=new Ae("");function oI(){wv(()=>{throw new Pe(600,!1)})}function aI(n){return n.isBoundToModule}var cI=10;var ln=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=Q(GT);afterRenderManager=Q(R0);zonelessEnabled=Q(jl);rootEffectScheduler=Q(sI);dirtyFlags=0;deferredDirtyFlags=0;tracingSnapshot=null;externalTestViews=new Set;afterTick=new Yt;get allViews(){return[...this.externalTestViews.keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];isStable=Q(Ai).hasPendingTasks.pipe(nt(t=>!t));constructor(){Q(no,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=Q(_n);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){let r=t instanceof Pl;if(!this._injector.get(Rx).done){let f=!r&&b_(t),h=!1;throw new Pe(405,h)}let o;r?o=t:o=this._injector.get(Zs).resolveComponentFactory(t),this.componentTypes.push(o.componentType);let a=aI(o)?void 0:this._injector.get(rr),c=i||o.selector,l=o.create(En.NULL,[],c,a),u=l.location.nativeElement,d=l.injector.get(Dx,null);return d?.registerApplication(u),l.onDestroy(()=>{this.detachView(l.hostView),hl(this.components,l),d?.unregisterApplication(u)}),this._loadComponent(l),l}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick=()=>{if(this.tracingSnapshot!==null){let i=this.tracingSnapshot;this.tracingSnapshot=null,i.run(yp.CHANGE_DETECTION,this._tick),i.dispose();return}if(this._runningTick)throw new Pe(101,!1);let t=gt(null);try{this._runningTick=!0,this.synchronize()}catch(i){this.internalErrorHandler(i)}finally{this._runningTick=!1,gt(t),this.afterTick.next()}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ks,null,{optional:!0})),this.dirtyFlags|=this.deferredDirtyFlags,this.deferredDirtyFlags=0;let t=0;for(;this.dirtyFlags!==0&&t++<cI;)this.synchronizeOnce()}synchronizeOnce(){if(this.dirtyFlags|=this.deferredDirtyFlags,this.deferredDirtyFlags=0,this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush()),this.dirtyFlags&7){let t=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:i,notifyErrorHandler:r}of this.allViews)lI(i,r,t,this.zonelessEnabled);if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}else this._rendererFactory?.begin?.(),this._rendererFactory?.end?.();this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>Hl(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;hl(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView),this.tick(),this.components.push(t),this._injector.get(Gr,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>hl(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new Pe(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function hl(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function lI(n,e,t,i){if(!t&&!Hl(n))return;dx(n,e,t&&!i?0:1)}function uI(n,e,t){let i=e[tr],r=e[qe];if(n.loadingState!==pn.NOT_STARTED)return n.loadingPromise??Promise.resolve();let s=$l(e,t),o=TC(r,n);n.loadingState=pn.IN_PROGRESS,fl(1,s);let a=n.dependencyResolverFn,c=i.get(Ai),l=c.add();return a?(n.loadingPromise=Promise.allSettled(a()).then(u=>{let d=!1,f=[],h=[];for(let g of u)if(g.status==="fulfilled"){let v=g.value,m=er(v)||Jh(v);if(m)f.push(m);else{let p=Qh(v);p&&h.push(p)}}else{d=!0;break}if(n.loadingPromise=null,c.remove(l),d){if(n.loadingState=pn.FAILED,n.errorTmplIndex===null){let g="",v=new Pe(-750,!1);Tp(e,v)}}else{n.loadingState=pn.COMPLETE;let g=o.tView;if(f.length>0){g.directiveRegistry=Wy(g.directiveRegistry,f);let v=f.map(p=>p.type),m=ep(!1,...v);n.providers=m}h.length>0&&(g.pipeRegistry=Wy(g.pipeRegistry,h))}}),n.loadingPromise):(n.loadingPromise=Promise.resolve().then(()=>{n.loadingPromise=null,n.loadingState=pn.COMPLETE,c.remove(l)}),n.loadingPromise)}function dI(n,e){return e[tr].get(eI,null,{optional:!0})?.behavior!==N0.Manual}function fI(n,e,t){let i=e[qe],r=e[t.index];if(!dI(n,e))return;let s=$l(e,t),o=ql(i,t);switch(MC(s),o.loadingState){case pn.NOT_STARTED:Bs(zt.Loading,t,r),uI(o,e,t),o.loadingState===pn.IN_PROGRESS&&r_(o,t,r);break;case pn.IN_PROGRESS:Bs(zt.Loading,t,r),r_(o,t,r);break;case pn.COMPLETE:Bs(zt.Complete,t,r);break;case pn.FAILED:Bs(zt.Error,t,r);break;default:}}function hI(n,e,t){return _i(this,null,function*(){let i=n.get(L0),r=i.hydrating;if(r.has(e))return;let{parentBlockPromise:s,hydrationQueue:o}=GC(e,n);pI(i,o);let a=n.get(Ai),c=a.add();s!==null&&(yield s);for(let l of o)yield gI(l,i),yield mI(n),r.get(l).resolve();yield r.get(e)?.promise,a.remove(c),t&&t(o),yA(i.get(e),o,i,n.get(ln))})}function pI(n,e){for(let t of e)n.hydrating.set(t,Promise.withResolvers())}function mI(n){return new Promise(e=>_p(e,{injector:n}))}function gI(n,e){return _i(this,null,function*(){let t=e.get(n);if(t===null)return;let{tNode:i,lView:r}=t,s=$l(r,i);return new Promise(o=>{vI(s,o),fI(2,r,i)})})}function vI(n,e){Array.isArray(n[Gs])||(n[Gs]=[]),n[Gs].push(e)}function s_(n,e,t,i,r){let s=e.inputs,o=r?"class":"style";ix(n,t,s[o],o,i)}function tu(n,e,t,i){let r=Ti(),s=B_(),o=nn+n,a=r[Zt],c=s.firstCreatePass?_x(o,s,r,e,xD,U_(),t,i):s.data[o],l=Nx(s,r,c,a,e,n);r[o]=l;let u=qb(c);return ap(c,!0),Y0(a,l,c),!io(c)&&mT()&&BD(s,r,l,c),tT()===0&&ha(l,r),nT(),u&&(lD(s,r,c),z0(s,c,r)),i!==null&&uD(r,c),tu}function nu(){let n=Vr();z_()?cT():(n=n.parent,ap(n,!1));let e=n;rT(e)&&oT(),iT();let t=B_();return t.firstCreatePass&&xx(t,e),e.classesWithoutHost!=null&&xT(e)&&s_(t,e,Ti(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&ET(e)&&s_(t,e,Ti(),e.stylesWithoutHost,!1),nu}function ar(n,e,t,i){return tu(n,e,t,i),nu(),ar}var Nx=(n,e,t,i,r,s)=>(sr(!0),Sp(i,r,Y_()));function yI(n,e,t,i,r,s){let o=e[ri],a=!o||da()||io(t)||pa(o,s);if(sr(a),a)return Sp(i,r,Y_());let c=Kl(o,n,e,t);return B0(o,s)&&Yl(o,s,c.nextSibling),o&&(g0(t)||v0(c))&&Js(t)&&(sT(t),X0(c)),c}function _I(){Nx=yI}var xI=(n,e,t,i)=>(sr(!0),$0(e[Zt],""));function EI(n,e,t,i){let r,s=e[ri],o=!s||da()||pa(s,i)||io(t);if(sr(o),o)return $0(e[Zt],"");let a=Kl(s,n,e,t),c=zC(s,i);return Yl(s,i,a),r=Jl(c,a),r}function MI(){xI=EI}var Fl="en-US";var SI=Fl;function wI(n){typeof n=="string"&&(SI=n.toLowerCase().replace(/_/g,"-"))}var bI=(n,e,t)=>{};function o_(n){bI=n}var TI=(n,e,t,i,r)=>(sr(!0),W0(e[Zt],i));function CI(n,e,t,i,r){let s=e[ri],o=!s||da()||io(t)||pa(s,r);return sr(o),o?W0(e[Zt],i):Kl(s,n,e,t)}function DI(){TI=CI}var qh=class{ngModuleFactory;componentFactories;constructor(e,t){this.ngModuleFactory=e,this.componentFactories=t}},Fp=(()=>{class n{compileModuleSync(t){return new Wh(t)}compileModuleAsync(t){return Promise.resolve(this.compileModuleSync(t))}compileModuleAndAllComponentsSync(t){let i=this.compileModuleSync(t),r=w_(t),s=H0(r.declarations).reduce((o,a)=>{let c=er(a);return c&&o.push(new ca(c)),o},[]);return new qh(i,s)}compileModuleAndAllComponentsAsync(t){return Promise.resolve(this.compileModuleAndAllComponentsSync(t))}clearCache(){}clearCacheFor(t){}getModuleId(t){}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var AI=(()=>{class n{zone=Q(kt);changeDetectionScheduler=Q(Xs);applicationRef=Q(ln);_onMicrotaskEmptySubscription;initialize(){this._onMicrotaskEmptySubscription||(this._onMicrotaskEmptySubscription=this.zone.onMicrotaskEmpty.subscribe({next:()=>{this.changeDetectionScheduler.runningTick||this.zone.run(()=>{this.applicationRef.tick()})}}))}ngOnDestroy(){this._onMicrotaskEmptySubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),II=new Ae("",{factory:()=>!1});function Px({ngZoneFactory:n,ignoreChangesOutsideZone:e,scheduleInRootZone:t}){return n??=()=>new kt(Mt(be({},Lx()),{scheduleInRootZone:t})),[{provide:kt,useFactory:n},{provide:Qi,multi:!0,useFactory:()=>{let i=Q(AI,{optional:!0});return()=>i.initialize()}},{provide:Qi,multi:!0,useFactory:()=>{let i=Q(RI);return()=>{i.initialize()}}},e===!0?{provide:d0,useValue:!0}:[],{provide:f0,useValue:t??u0}]}function Ox(n){let e=n?.ignoreChangesOutsideZone,t=n?.scheduleInRootZone,i=Px({ngZoneFactory:()=>{let r=Lx(n);return r.scheduleInRootZone=t,r.shouldCoalesceEventChangeDetection&&or("NgZone_CoalesceEvent"),new kt(r)},ignoreChangesOutsideZone:e,scheduleInRootZone:t});return Br([{provide:II,useValue:!0},{provide:jl,useValue:!1},i])}function Lx(n){return{enableLongStackTrace:!1,shouldCoalesceEventChangeDetection:n?.eventCoalescing??!1,shouldCoalesceRunChangeDetection:n?.runCoalescing??!1}}var RI=(()=>{class n{subscription=new Rt;initialized=!1;zone=Q(kt);pendingTasks=Q(Ai);initialize(){if(this.initialized)return;this.initialized=!0;let t=null;!this.zone.isStable&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(t=this.pendingTasks.add()),this.zone.runOutsideAngular(()=>{this.subscription.add(this.zone.onStable.subscribe(()=>{kt.assertNotInAngularZone(),queueMicrotask(()=>{t!==null&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(this.pendingTasks.remove(t),t=null)})}))}),this.subscription.add(this.zone.onUnstable.subscribe(()=>{kt.assertInAngularZone(),t??=this.pendingTasks.add()}))}ngOnDestroy(){this.subscription.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var NI=(()=>{class n{appRef=Q(ln);taskService=Q(Ai);ngZone=Q(kt);zonelessEnabled=Q(jl);tracing=Q(no,{optional:!0});disableScheduling=Q(d0,{optional:!0})??!1;zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new Rt;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Tl):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(Q(f0,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{this.runningTick||this.cleanup()})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()})),this.disableScheduling||=!this.zonelessEnabled&&(this.ngZone instanceof Nh||!this.zoneIsDefined)}notify(t){if(!this.zonelessEnabled&&t===5)return;let i=!1;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 8:{this.appRef.deferredDirtyFlags|=8;break}case 6:{this.appRef.dirtyFlags|=2,i=!0;break}case 13:{this.appRef.dirtyFlags|=16,i=!0;break}case 14:{this.appRef.dirtyFlags|=2,i=!0;break}case 12:{i=!0;break}case 10:case 9:case 7:case 11:default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick(i))return;let r=this.useMicrotaskScheduler?Fy:h0;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(t){return!(this.disableScheduling&&!t||this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Tl+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){throw this.taskService.remove(t),i}finally{this.cleanup()}this.useMicrotaskScheduler=!0,Fy(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function PI(){return typeof $localize<"u"&&$localize.locale||Fl}var kp=new Ae("",{providedIn:"root",factory:()=>Q(kp,Ke.Optional|Ke.SkipSelf)||PI()});var Xh=new Ae(""),OI=new Ae("");function Jo(n){return!n.moduleRef}function LI(n){let e=Jo(n)?n.r3Injector:n.moduleRef.injector,t=e.get(kt);return t.run(()=>{Jo(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(Ci,null),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:s=>{i.handleError(s)}})}),Jo(n)){let s=()=>e.destroy(),o=n.platformInjector.get(Xh);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(Xh);o.add(s),n.moduleRef.onDestroy(()=>{hl(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return kI(i,t,()=>{let s=e.get(Rx);return s.runInitializers(),s.donePromise.then(()=>{let o=e.get(kp,Fl);if(wI(o||Fl),!e.get(OI,!0))return Jo(n)?e.get(ln):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Jo(n)){let c=e.get(ln);return n.rootComponent!==void 0&&c.bootstrap(n.rootComponent),c}else return FI(n.moduleRef,n.allPlatformModules),n.moduleRef})})})}function FI(n,e){let t=n.injector.get(ln);if(n._bootstrapComponents.length>0)n._bootstrapComponents.forEach(i=>t.bootstrap(i));else if(n.instance.ngDoBootstrap)n.instance.ngDoBootstrap(t);else throw new Pe(-403,!1);e.push(n)}function kI(n,e,t){try{let i=t();return ma(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n.handleError(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n.handleError(i)),i}}var pl=null;function UI(n=[],e){return En.create({name:e,providers:[{provide:Vl,useValue:"platform"},{provide:Xh,useValue:new Set([()=>pl=null])},...n]})}function BI(n=[]){if(pl)return pl;let e=UI(n);return pl=e,oI(),VI(e),e}function VI(n){let e=n.get(mp,null);Fn(n,()=>{e?.forEach(t=>t())})}var ga=(()=>{class n{static __NG_ELEMENT_ID__=zI}return n})();function zI(n){return HI(Vr(),Ti(),(n&16)===16)}function HI(n,e,t){if(Js(n)&&!t){let i=Lr(n.index,e);return new Ys(i,i)}else if(n.type&175){let i=e[Ln];return new Ys(i,e)}return null}function Fx(n){try{let{rootComponent:e,appProviders:t,platformProviders:i}=n,r=BI(i),s=[Px({}),{provide:Xs,useExisting:NI},...t||[]],o=new Ll({providers:s,parent:r,debugName:"",runEnvironmentInitializers:!1});return LI({r3Injector:o.injector,platformInjector:r,rootComponent:e})}catch(e){return Promise.reject(e)}}var cl=new WeakSet,a_="",ml=[];function c_(n){return n.get(I0,uC)}function kx(){let n=[{provide:I0,useFactory:()=>{let e=!0;{let t=Q(Ur);e=!!window._ejsas?.[t]}return e&&or("NgEventReplay"),e}}];return n.push({provide:Qi,useValue:()=>{let e=Q(En),t=e.get(ln);if(!cl.has(t)){let i=Q(Hy);c_(e)&&o_((r,s,o)=>{CC(r,s,o),DC(r,i)})}},multi:!0},{provide:Gr,useFactory:()=>{let e=Q(Ur),t=Q(En),i=Q(ln);return()=>{!c_(t)||cl.has(i)||(cl.add(i),i.onDestroy(()=>{cl.delete(i),sh(e),o_(()=>{})}),i.whenStable().then(()=>{let r=t.get(IC);GI(r,t);let s=t.get(Hy);s.get(a_)?.forEach(AC),s.delete(a_);let o=r.instance;VC(t)?i.onDestroy(()=>o.cleanUp()):o.cleanUp()}))}},multi:!0}),n}var GI=(n,e)=>{let t=e.get(Ur),i=window._ejsas[t],r=n.instance=new vy(new sl(i.c));for(let a of i.et)r.addEvent(a);for(let a of i.etc)r.addEvent(a);let s=yy(t);r.replayEarlyEventInfos(s),sh(t);let o=new ol(a=>{jI(e,a,a.currentTarget)});gy(r,o)};function jI(n,e,t){let i=(t&&t.getAttribute(Xl))??"";/d\d+/.test(i)?WI(i,n,e,t):e.eventPhase===rh.REPLAY&&O0(e,t)}function WI(n,e,t,i){ml.push({event:t,currentTarget:i}),hI(e,n,$I)}function $I(n){let e=[...ml],t=new Set(n);ml=[];for(let{event:i,currentTarget:r}of e){let s=r.getAttribute(Xl);t.has(s)?O0(i,r):ml.push({event:i,currentTarget:r})}}var l_=!1;function qI(){l_||(l_=!0,FC(),_I(),DI(),MI(),JA(),WA(),EA(),gD())}function XI(n){return n.whenStable()}function Ux(){let n=[{provide:al,useFactory:()=>{let e=!0;return e=!!Q(zr,{optional:!0})?.get(F0,null),e&&or("NgHydration"),e}},{provide:Qi,useValue:()=>{pA(!1),Q(al)&&(YI(),qI())},multi:!0}];return n.push({provide:A0,useFactory:()=>Q(al)},{provide:Gr,useFactory:()=>{if(Q(al)){let e=Q(ln);return()=>{XI(e).then(()=>{e.destroyed||gx(e)})}}return()=>{}},multi:!0}),Br(n)}function YI(){let n=Wl(),e;for(let t of n.body.childNodes)if(t.nodeType===Node.COMMENT_NODE&&t.textContent?.trim()===OC){e=t;break}if(!e)throw new Pe(-507,!1)}var u_=class{[Ss];constructor(e){this[Ss]=e}destroy(){this[Ss].destroy()}};var Wx=null;function oo(){return Wx}function $x(n){Wx??=n}var iu=class{};var kn=new Ae(""),qx=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:()=>Q(eR),providedIn:"platform"})}return n})();var eR=(()=>{class n extends qx{_location;_history;_doc=Q(kn);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return oo().getBaseHref(this._doc)}onPopState(t){let i=oo().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",t,!1),()=>i.removeEventListener("popstate",t)}onHashChange(t){let i=oo().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",t,!1),()=>i.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,i,r){this._history.pushState(t,i,r)}replaceState(t,i,r){this._history.replaceState(t,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:()=>new n,providedIn:"platform"})}return n})();function Xx(n,e){return n?e?n.endsWith("/")?e.startsWith("/")?n+e.slice(1):n+e:e.startsWith("/")?n+e:`${n}/${e}`:n:e}function Bx(n){let e=n.match(/#|\?|$/),t=e&&e.index||n.length,i=t-(n[t-1]==="/"?1:0);return n.slice(0,i)+n.slice(t)}function jr(n){return n&&n[0]!=="?"?"?"+n:n}var su=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:()=>Q(Yx),providedIn:"root"})}return n})(),tR=new Ae(""),Yx=(()=>{class n extends su{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,i){super(),this._platformLocation=t,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??Q(kn).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return Xx(this._baseHref,t)}path(t=!1){let i=this._platformLocation.pathname+jr(this._platformLocation.search),r=this._platformLocation.hash;return r&&t?`${i}${r}`:i}pushState(t,i,r,s){let o=this.prepareExternalUrl(r+jr(s));this._platformLocation.pushState(t,i,o)}replaceState(t,i,r,s){let o=this.prepareExternalUrl(r+jr(s));this._platformLocation.replaceState(t,i,o)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(i){return new(i||n)(et(qx),et(tR,8))};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var ya=(()=>{class n{_subject=new Yt;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let i=this._locationStrategy.getBaseHref();this._basePath=rR(Bx(Vx(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,i=""){return this.path()==this.normalize(t+jr(i))}normalize(t){return n.stripTrailingSlash(iR(this._basePath,Vx(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,i="",r=null){this._locationStrategy.pushState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+jr(i)),r)}replaceState(t,i="",r=null){this._locationStrategy.replaceState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+jr(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",i){this._urlChangeListeners.forEach(r=>r(t,i))}subscribe(t,i,r){return this._subject.subscribe({next:t,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=jr;static joinWithSlash=Xx;static stripTrailingSlash=Bx;static \u0275fac=function(i){return new(i||n)(et(su))};static \u0275prov=ke({token:n,factory:()=>nR(),providedIn:"root"})}return n})();function nR(){return new ya(et(su))}function iR(n,e){if(!n||!e.startsWith(n))return e;let t=e.substring(n.length);return t===""||["/",";","?","#"].includes(t[0])?t:e}function Vx(n){return n.replace(/\/index.html$/,"")}function rR(n){if(new RegExp("^(https?:)?//").test(n)){let[,t]=n.split(/\/\/[^\/]+/);return t}return n}function Zx(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var Up="browser",sR="server";function Bp(n){return n===Up}function Vp(n){return n===sR}var ru=class{};var au=class n{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(e){e?typeof e=="string"?this.lazyInit=()=>{this.headers=new Map,e.split(`
`).forEach(t=>{let i=t.indexOf(":");if(i>0){let r=t.slice(0,i),s=t.slice(i+1).trim();this.addHeaderEntry(r,s)}})}:typeof Headers<"u"&&e instanceof Headers?(this.headers=new Map,e.forEach((t,i)=>{this.addHeaderEntry(i,t)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(e).forEach(([t,i])=>{this.setHeaderEntries(t,i)})}:this.headers=new Map}has(e){return this.init(),this.headers.has(e.toLowerCase())}get(e){this.init();let t=this.headers.get(e.toLowerCase());return t&&t.length>0?t[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(e){return this.init(),this.headers.get(e.toLowerCase())||null}append(e,t){return this.clone({name:e,value:t,op:"a"})}set(e,t){return this.clone({name:e,value:t,op:"s"})}delete(e,t){return this.clone({name:e,value:t,op:"d"})}maybeSetNormalizedName(e,t){this.normalizedNames.has(t)||this.normalizedNames.set(t,e)}init(){this.lazyInit&&(this.lazyInit instanceof n?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(e=>this.applyUpdate(e)),this.lazyUpdate=null))}copyFrom(e){e.init(),Array.from(e.headers.keys()).forEach(t=>{this.headers.set(t,e.headers.get(t)),this.normalizedNames.set(t,e.normalizedNames.get(t))})}clone(e){let t=new n;return t.lazyInit=this.lazyInit&&this.lazyInit instanceof n?this.lazyInit:this,t.lazyUpdate=(this.lazyUpdate||[]).concat([e]),t}applyUpdate(e){let t=e.name.toLowerCase();switch(e.op){case"a":case"s":let i=e.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(e.name,t);let r=(e.op==="a"?this.headers.get(t):void 0)||[];r.push(...i),this.headers.set(t,r);break;case"d":let s=e.value;if(!s)this.headers.delete(t),this.normalizedNames.delete(t);else{let o=this.headers.get(t);if(!o)return;o=o.filter(a=>s.indexOf(a)===-1),o.length===0?(this.headers.delete(t),this.normalizedNames.delete(t)):this.headers.set(t,o)}break}}addHeaderEntry(e,t){let i=e.toLowerCase();this.maybeSetNormalizedName(e,i),this.headers.has(i)?this.headers.get(i).push(t):this.headers.set(i,[t])}setHeaderEntries(e,t){let i=(Array.isArray(t)?t:[t]).map(s=>s.toString()),r=e.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(e,r)}forEach(e){this.init(),Array.from(this.normalizedNames.keys()).forEach(t=>e(this.normalizedNames.get(t),this.headers.get(t)))}};var oR="X-Request-URL",aR="text/plain",cR="application/json",YH=`${cR}, ${aR}, */*`;var Jx=function(n){return n[n.Sent=0]="Sent",n[n.UploadProgress=1]="UploadProgress",n[n.ResponseHeader=2]="ResponseHeader",n[n.DownloadProgress=3]="DownloadProgress",n[n.Response=4]="Response",n[n.User=5]="User",n}(Jx||{}),zp=class{headers;status;statusText;url;ok;type;constructor(e,t=200,i="OK"){this.headers=e.headers||new au,this.status=e.status!==void 0?e.status:t,this.statusText=e.statusText||i,this.url=e.url||null,this.ok=this.status>=200&&this.status<300}};var cu=class n extends zp{body;constructor(e={}){super(e),this.body=e.body!==void 0?e.body:null}type=Jx.Response;clone(e={}){return new n({body:e.body!==void 0?e.body:this.body,headers:e.headers||this.headers,status:e.status!==void 0?e.status:this.status,statusText:e.statusText||this.statusText,url:e.url||this.url||void 0})}};var lR=new Ae("");var ZH=RegExp(`^${oR}:`,"m");var uR=new Ae(""),dR="b",fR="h",hR="s",pR="st",mR="u",gR="rt",ou=new Ae(""),vR=["GET","HEAD"];function yR(n,e){let f=Q(ou),{isCacheActive:t}=f,i=pv(f,["isCacheActive"]),{transferCache:r,method:s}=n;if(!t||r===!1||s==="POST"&&!i.includePostRequests&&!r||s!=="POST"&&!vR.includes(s)||!i.includeRequestsWithAuthHeaders&&_R(n)||i.filter?.(n)===!1)return e(n);let o=Q(zr);if(Q(uR,{optional:!0}))throw new Pe(2803,!1);let c=n.url,l=xR(n,c),u=o.get(l,null),d=i.includeHeaders;if(typeof r=="object"&&r.includeHeaders&&(d=r.includeHeaders),u){let{[dR]:h,[gR]:g,[fR]:v,[hR]:m,[pR]:p,[mR]:T}=u,M=h;switch(g){case"arraybuffer":M=new TextEncoder().encode(h).buffer;break;case"blob":M=new Blob([h]);break}let E=new au(v);return ze(new cu({body:M,headers:E,status:m,statusText:p,url:T}))}return e(n).pipe(Ft(h=>{h instanceof cu}))}function _R(n){return n.headers.has("authorization")||n.headers.has("proxy-authorization")}function Kx(n){return[...n.keys()].sort().map(e=>`${e}=${n.getAll(e)}`).join("&")}function xR(n,e){let{params:t,method:i,responseType:r}=n,s=Kx(t),o=n.serializeBody();o instanceof URLSearchParams?o=Kx(o):typeof o!="string"&&(o="");let a=[i,r,e,o,s].join("|"),c=ER(a);return c}function ER(n){let e=0;for(let t of n)e=Math.imul(31,e)+t.charCodeAt(0)<<0;return e+=2147483648,e.toString()}function Qx(n){return[{provide:ou,useFactory:()=>(or("NgHttpTransferCache"),be({isCacheActive:!0},n))},{provide:lR,useValue:yR,multi:!0,deps:[zr,ou]},{provide:Gr,multi:!0,useFactory:()=>{let e=Q(ln),t=Q(ou);return()=>{e.whenStable().then(()=>{t.isCacheActive=!1})}}}]}var Gp=class extends iu{supportsDOMEvents=!0},jp=class n extends Gp{static makeCurrent(){$x(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=MR();return t==null?null:SR(t)}resetBaseElement(){_a=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return Zx(document.cookie,e)}},_a=null;function MR(){return _a=_a||document.querySelector("base"),_a?_a.getAttribute("href"):null}function SR(n){return new URL(n,document.baseURI).pathname}var wR=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:n.\u0275fac})}return n})(),Wp=new Ae(""),sE=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(r=>{r.manager=this}),this._plugins=t.slice().reverse()}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new Pe(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(et(Wp),et(kt))};static \u0275prov=ke({token:n,factory:n.\u0275fac})}return n})(),uu=class{_doc;constructor(e){this._doc=e}manager},lu="ng-app-id";function eE(n){for(let e of n)e.remove()}function tE(n,e){let t=e.createElement("style");return t.textContent=n,t}function bR(n,e,t,i){let r=n.head?.querySelectorAll(`style[${lu}="${e}"],link[${lu}="${e}"]`);if(r)for(let s of r)s.removeAttribute(lu),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function $p(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var oE=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;isServer;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,this.isServer=Vp(s),bR(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,tE);i?.forEach(r=>this.addUsage(r,this.external,$p))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(eE(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])eE(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,tE(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,$p(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),this.isServer&&i.setAttribute(lu,this.appId),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(et(kn),et(Ur),et(gp,8),et(Ii))};static \u0275prov=ke({token:n,factory:n.\u0275fac})}return n})(),Hp={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Xp=/%COMP%/g;var aE="%COMP%",TR=`_nghost-${aE}`,CR=`_ngcontent-${aE}`,DR=!0,AR=new Ae("",{providedIn:"root",factory:()=>DR});function IR(n){return CR.replace(Xp,n)}function RR(n){return TR.replace(Xp,n)}function cE(n,e){return e.map(t=>t.replace(Xp,n))}var nE=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;platformId;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;platformIsServer;constructor(t,i,r,s,o,a,c,l=null,u=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.platformId=a,this.ngZone=c,this.nonce=l,this.tracingService=u,this.platformIsServer=Vp(a),this.defaultRenderer=new xa(t,o,c,this.platformIsServer,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;this.platformIsServer&&i.encapsulation===si.ShadowDom&&(i=Mt(be({},i),{encapsulation:si.Emulated}));let r=this.getOrCreateRenderer(t,i);return r instanceof du?r.applyToHost(t):r instanceof Ea&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.platformIsServer,f=this.tracingService;switch(i.encapsulation){case si.Emulated:s=new du(c,l,i,this.appId,u,o,a,d,f);break;case si.ShadowDom:return new qp(c,l,t,i,o,a,this.nonce,d,f);default:s=new Ea(c,l,i,u,o,a,d,f);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(et(sE),et(oE),et(Ur),et(AR),et(kn),et(Ii),et(kt),et(gp),et(no,8))};static \u0275prov=ke({token:n,factory:n.\u0275fac})}return n})(),xa=class{eventManager;doc;ngZone;platformIsServer;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r,s){this.eventManager=e,this.doc=t,this.ngZone=i,this.platformIsServer=r,this.tracingService=s}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(Hp[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(iE(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(iE(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new Pe(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=Hp[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=Hp[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(Hr.DashCase|Hr.Important)?e.style.setProperty(t,i,r&Hr.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&Hr.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=oo().getGlobalEventTarget(this.doc,e),!e))throw new Error(`Unsupported event target ${e} for event ${t}`);let s=this.decoratePreventDefault(i);return this.tracingService!==null&&this.tracingService.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;(this.platformIsServer?this.ngZone.runGuarded(()=>e(t)):e(t))===!1&&t.preventDefault()}}};function iE(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var qp=class extends xa{sharedStylesHost;hostEl;shadowRoot;constructor(e,t,i,r,s,o,a,c,l){super(e,s,o,c,l),this.sharedStylesHost=t,this.hostEl=i,this.shadowRoot=i.attachShadow({mode:"open"}),this.sharedStylesHost.addHost(this.shadowRoot);let u=r.styles;u=cE(r.id,u);for(let f of u){let h=document.createElement("style");a&&h.setAttribute("nonce",a),h.textContent=f,this.shadowRoot.appendChild(h)}let d=r.getExternalStyles?.();if(d)for(let f of d){let h=$p(f,s);a&&h.setAttribute("nonce",a),this.shadowRoot.appendChild(h)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost.removeHost(this.shadowRoot)}},Ea=class extends xa{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c,l){super(e,s,o,a,c),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let u=i.styles;this.styles=l?cE(l,u):u,this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},du=class extends Ea{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c,l){let u=r+"-"+i.id;super(e,t,i,s,o,a,c,l,u),this.contentAttr=IR(u),this.hostAttr=RR(u)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}},NR=(()=>{class n extends uu{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(et(kn))};static \u0275prov=ke({token:n,factory:n.\u0275fac})}return n})(),rE=["alt","control","meta","shift"],PR={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},OR={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},LR=(()=>{class n extends uu{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>oo().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),rE.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=PR[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),rE.forEach(o=>{if(o!==r){let a=OR[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(et(kn))};static \u0275prov=ke({token:n,factory:n.\u0275fac})}return n})();function lE(n,e){return Fx(be({rootComponent:n},FR(e)))}function FR(n){return{appProviders:[...zR,...n?.providers??[]],platformProviders:VR}}function kR(){jp.makeCurrent()}function UR(){return new Ci}function BR(){return S0(document),document}var VR=[{provide:Ii,useValue:Up},{provide:mp,useValue:kR,multi:!0},{provide:kn,useFactory:BR,deps:[]}];var zR=[{provide:Vl,useValue:"root"},{provide:Ci,useFactory:UR,deps:[]},{provide:Wp,useClass:NR,multi:!0,deps:[kn]},{provide:Wp,useClass:LR,multi:!0,deps:[kn]},nE,oE,sE,{provide:Ks,useExisting:nE},{provide:ru,useClass:wR,deps:[]},[]];var uE=(()=>{class n{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(i){return new(i||n)(et(kn))};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var fu=function(n){return n[n.NoHttpTransferCache=0]="NoHttpTransferCache",n[n.HttpTransferCacheOptions=1]="HttpTransferCacheOptions",n[n.I18nSupport=2]="I18nSupport",n[n.EventReplay=3]="EventReplay",n[n.IncrementalHydration=4]="IncrementalHydration",n}(fu||{});function HR(n,e=[],t={}){return{\u0275kind:n,\u0275providers:e}}function dE(){return HR(fu.EventReplay,kx())}function fE(...n){let e=[],t=new Set,i=t.has(fu.HttpTransferCacheOptions);for(let{\u0275providers:r,\u0275kind:s}of n)t.add(s),r.length&&e.push(r);return Br([[],Ux(),t.has(fu.NoHttpTransferCache)||i?[]:Qx({}),e])}var Xe="primary",ka=Symbol("RouteTitle"),Qp=class{params;constructor(e){this.params=e||{}}has(e){return Object.prototype.hasOwnProperty.call(this.params,e)}get(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t[0]:t}return null}getAll(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t:[t]}return[]}get keys(){return Object.keys(this.params)}};function ho(n){return new Qp(n)}function jR(n,e,t){let i=t.path.split("/");if(i.length>n.length||t.pathMatch==="full"&&(e.hasChildren()||i.length<n.length))return null;let r={};for(let s=0;s<i.length;s++){let o=i[s],a=n[s];if(o[0]===":")r[o.substring(1)]=a;else if(o!==a.path)return null}return{consumed:n.slice(0,i.length),posParams:r}}function WR(n,e){if(n.length!==e.length)return!1;for(let t=0;t<n.length;++t)if(!ai(n[t],e[t]))return!1;return!0}function ai(n,e){let t=n?em(n):void 0,i=e?em(e):void 0;if(!t||!i||t.length!=i.length)return!1;let r;for(let s=0;s<t.length;s++)if(r=t[s],!xE(n[r],e[r]))return!1;return!0}function em(n){return[...Object.keys(n),...Object.getOwnPropertySymbols(n)]}function xE(n,e){if(Array.isArray(n)&&Array.isArray(e)){if(n.length!==e.length)return!1;let t=[...n].sort(),i=[...e].sort();return t.every((r,s)=>i[s]===r)}else return n===e}function EE(n){return n.length>0?n[n.length-1]:null}function cr(n){return jf(n)?n:ma(n)?Nt(Promise.resolve(n)):ze(n)}var $R={exact:SE,subset:wE},ME={exact:qR,subset:XR,ignored:()=>!0};function hE(n,e,t){return $R[t.paths](n.root,e.root,t.matrixParams)&&ME[t.queryParams](n.queryParams,e.queryParams)&&!(t.fragment==="exact"&&n.fragment!==e.fragment)}function qR(n,e){return ai(n,e)}function SE(n,e,t){if(!$r(n.segments,e.segments)||!mu(n.segments,e.segments,t)||n.numberOfChildren!==e.numberOfChildren)return!1;for(let i in e.children)if(!n.children[i]||!SE(n.children[i],e.children[i],t))return!1;return!0}function XR(n,e){return Object.keys(e).length<=Object.keys(n).length&&Object.keys(e).every(t=>xE(n[t],e[t]))}function wE(n,e,t){return bE(n,e,e.segments,t)}function bE(n,e,t,i){if(n.segments.length>t.length){let r=n.segments.slice(0,t.length);return!(!$r(r,t)||e.hasChildren()||!mu(r,t,i))}else if(n.segments.length===t.length){if(!$r(n.segments,t)||!mu(n.segments,t,i))return!1;for(let r in e.children)if(!n.children[r]||!wE(n.children[r],e.children[r],i))return!1;return!0}else{let r=t.slice(0,n.segments.length),s=t.slice(n.segments.length);return!$r(n.segments,r)||!mu(n.segments,r,i)||!n.children[Xe]?!1:bE(n.children[Xe],e,s,i)}}function mu(n,e,t){return e.every((i,r)=>ME[t](n[r].parameters,i.parameters))}var Ni=class{root;queryParams;fragment;_queryParamMap;constructor(e=new pt([],{}),t={},i=null){this.root=e,this.queryParams=t,this.fragment=i}get queryParamMap(){return this._queryParamMap??=ho(this.queryParams),this._queryParamMap}toString(){return KR.serialize(this)}},pt=class{segments;children;parent=null;constructor(e,t){this.segments=e,this.children=t,Object.values(t).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return gu(this)}},Wr=class{path;parameters;_parameterMap;constructor(e,t){this.path=e,this.parameters=t}get parameterMap(){return this._parameterMap??=ho(this.parameters),this._parameterMap}toString(){return CE(this)}};function YR(n,e){return $r(n,e)&&n.every((t,i)=>ai(t.parameters,e[i].parameters))}function $r(n,e){return n.length!==e.length?!1:n.every((t,i)=>t.path===e[i].path)}function ZR(n,e){let t=[];return Object.entries(n.children).forEach(([i,r])=>{i===Xe&&(t=t.concat(e(r,i)))}),Object.entries(n.children).forEach(([i,r])=>{i!==Xe&&(t=t.concat(e(r,i)))}),t}var Tm=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:()=>new Ca,providedIn:"root"})}return n})(),Ca=class{parse(e){let t=new nm(e);return new Ni(t.parseRootSegment(),t.parseQueryParams(),t.parseFragment())}serialize(e){let t=`/${Ma(e.root,!0)}`,i=e1(e.queryParams),r=typeof e.fragment=="string"?`#${JR(e.fragment)}`:"";return`${t}${i}${r}`}},KR=new Ca;function gu(n){return n.segments.map(e=>CE(e)).join("/")}function Ma(n,e){if(!n.hasChildren())return gu(n);if(e){let t=n.children[Xe]?Ma(n.children[Xe],!1):"",i=[];return Object.entries(n.children).forEach(([r,s])=>{r!==Xe&&i.push(`${r}:${Ma(s,!1)}`)}),i.length>0?`${t}(${i.join("//")})`:t}else{let t=ZR(n,(i,r)=>r===Xe?[Ma(n.children[Xe],!1)]:[`${r}:${Ma(i,!1)}`]);return Object.keys(n.children).length===1&&n.children[Xe]!=null?`${gu(n)}/${t[0]}`:`${gu(n)}/(${t.join("//")})`}}function TE(n){return encodeURIComponent(n).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function hu(n){return TE(n).replace(/%3B/gi,";")}function JR(n){return encodeURI(n)}function tm(n){return TE(n).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function vu(n){return decodeURIComponent(n)}function pE(n){return vu(n.replace(/\+/g,"%20"))}function CE(n){return`${tm(n.path)}${QR(n.parameters)}`}function QR(n){return Object.entries(n).map(([e,t])=>`;${tm(e)}=${tm(t)}`).join("")}function e1(n){let e=Object.entries(n).map(([t,i])=>Array.isArray(i)?i.map(r=>`${hu(t)}=${hu(r)}`).join("&"):`${hu(t)}=${hu(i)}`).filter(t=>t);return e.length?`?${e.join("&")}`:""}var t1=/^[^\/()?;#]+/;function Yp(n){let e=n.match(t1);return e?e[0]:""}var n1=/^[^\/()?;=#]+/;function i1(n){let e=n.match(n1);return e?e[0]:""}var r1=/^[^=?&#]+/;function s1(n){let e=n.match(r1);return e?e[0]:""}var o1=/^[^&#]+/;function a1(n){let e=n.match(o1);return e?e[0]:""}var nm=class{url;remaining;constructor(e){this.url=e,this.remaining=e}parseRootSegment(){return this.consumeOptional("/"),this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new pt([],{}):new pt([],this.parseChildren())}parseQueryParams(){let e={};if(this.consumeOptional("?"))do this.parseQueryParam(e);while(this.consumeOptional("&"));return e}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(){if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let t={};this.peekStartsWith("/(")&&(this.capture("/"),t=this.parseParens(!0));let i={};return this.peekStartsWith("(")&&(i=this.parseParens(!1)),(e.length>0||Object.keys(t).length>0)&&(i[Xe]=new pt(e,t)),i}parseSegment(){let e=Yp(this.remaining);if(e===""&&this.peekStartsWith(";"))throw new Pe(4009,!1);return this.capture(e),new Wr(vu(e),this.parseMatrixParams())}parseMatrixParams(){let e={};for(;this.consumeOptional(";");)this.parseParam(e);return e}parseParam(e){let t=i1(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let r=Yp(this.remaining);r&&(i=r,this.capture(i))}e[vu(t)]=vu(i)}parseQueryParam(e){let t=s1(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let o=a1(this.remaining);o&&(i=o,this.capture(i))}let r=pE(t),s=pE(i);if(e.hasOwnProperty(r)){let o=e[r];Array.isArray(o)||(o=[o],e[r]=o),o.push(s)}else e[r]=s}parseParens(e){let t={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let i=Yp(this.remaining),r=this.remaining[i.length];if(r!=="/"&&r!==")"&&r!==";")throw new Pe(4010,!1);let s;i.indexOf(":")>-1?(s=i.slice(0,i.indexOf(":")),this.capture(s),this.capture(":")):e&&(s=Xe);let o=this.parseChildren();t[s]=Object.keys(o).length===1?o[Xe]:new pt([],o),this.consumeOptional("//")}return t}peekStartsWith(e){return this.remaining.startsWith(e)}consumeOptional(e){return this.peekStartsWith(e)?(this.remaining=this.remaining.substring(e.length),!0):!1}capture(e){if(!this.consumeOptional(e))throw new Pe(4011,!1)}};function DE(n){return n.segments.length>0?new pt([],{[Xe]:n}):n}function AE(n){let e={};for(let[i,r]of Object.entries(n.children)){let s=AE(r);if(i===Xe&&s.segments.length===0&&s.hasChildren())for(let[o,a]of Object.entries(s.children))e[o]=a;else(s.segments.length>0||s.hasChildren())&&(e[i]=s)}let t=new pt(n.segments,e);return c1(t)}function c1(n){if(n.numberOfChildren===1&&n.children[Xe]){let e=n.children[Xe];return new pt(n.segments.concat(e.segments),e.children)}return n}function Da(n){return n instanceof Ni}function l1(n,e,t=null,i=null){let r=IE(n);return RE(r,e,t,i)}function IE(n){let e;function t(s){let o={};for(let c of s.children){let l=t(c);o[c.outlet]=l}let a=new pt(s.url,o);return s===n&&(e=a),a}let i=t(n.root),r=DE(i);return e??r}function RE(n,e,t,i){let r=n;for(;r.parent;)r=r.parent;if(e.length===0)return Zp(r,r,r,t,i);let s=u1(e);if(s.toRoot())return Zp(r,r,new pt([],{}),t,i);let o=d1(s,r,n),a=o.processChildren?wa(o.segmentGroup,o.index,s.commands):PE(o.segmentGroup,o.index,s.commands);return Zp(r,o.segmentGroup,a,t,i)}function yu(n){return typeof n=="object"&&n!=null&&!n.outlets&&!n.segmentPath}function Aa(n){return typeof n=="object"&&n!=null&&n.outlets}function Zp(n,e,t,i,r){let s={};i&&Object.entries(i).forEach(([c,l])=>{s[c]=Array.isArray(l)?l.map(u=>`${u}`):`${l}`});let o;n===e?o=t:o=NE(n,e,t);let a=DE(AE(o));return new Ni(a,s,r)}function NE(n,e,t){let i={};return Object.entries(n.children).forEach(([r,s])=>{s===e?i[r]=t:i[r]=NE(s,e,t)}),new pt(n.segments,i)}var _u=class{isAbsolute;numberOfDoubleDots;commands;constructor(e,t,i){if(this.isAbsolute=e,this.numberOfDoubleDots=t,this.commands=i,e&&i.length>0&&yu(i[0]))throw new Pe(4003,!1);let r=i.find(Aa);if(r&&r!==EE(i))throw new Pe(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function u1(n){if(typeof n[0]=="string"&&n.length===1&&n[0]==="/")return new _u(!0,0,n);let e=0,t=!1,i=n.reduce((r,s,o)=>{if(typeof s=="object"&&s!=null){if(s.outlets){let a={};return Object.entries(s.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(s.segmentPath)return[...r,s.segmentPath]}return typeof s!="string"?[...r,s]:o===0?(s.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?t=!0:a===".."?e++:a!=""&&r.push(a))}),r):[...r,s]},[]);return new _u(t,e,i)}var lo=class{segmentGroup;processChildren;index;constructor(e,t,i){this.segmentGroup=e,this.processChildren=t,this.index=i}};function d1(n,e,t){if(n.isAbsolute)return new lo(e,!0,0);if(!t)return new lo(e,!1,NaN);if(t.parent===null)return new lo(t,!0,0);let i=yu(n.commands[0])?0:1,r=t.segments.length-1+i;return f1(t,r,n.numberOfDoubleDots)}function f1(n,e,t){let i=n,r=e,s=t;for(;s>r;){if(s-=r,i=i.parent,!i)throw new Pe(4005,!1);r=i.segments.length}return new lo(i,!1,r-s)}function h1(n){return Aa(n[0])?n[0].outlets:{[Xe]:n}}function PE(n,e,t){if(n??=new pt([],{}),n.segments.length===0&&n.hasChildren())return wa(n,e,t);let i=p1(n,e,t),r=t.slice(i.commandIndex);if(i.match&&i.pathIndex<n.segments.length){let s=new pt(n.segments.slice(0,i.pathIndex),{});return s.children[Xe]=new pt(n.segments.slice(i.pathIndex),n.children),wa(s,0,r)}else return i.match&&r.length===0?new pt(n.segments,{}):i.match&&!n.hasChildren()?im(n,e,t):i.match?wa(n,0,r):im(n,e,t)}function wa(n,e,t){if(t.length===0)return new pt(n.segments,{});{let i=h1(t),r={};if(Object.keys(i).some(s=>s!==Xe)&&n.children[Xe]&&n.numberOfChildren===1&&n.children[Xe].segments.length===0){let s=wa(n.children[Xe],e,t);return new pt(n.segments,s.children)}return Object.entries(i).forEach(([s,o])=>{typeof o=="string"&&(o=[o]),o!==null&&(r[s]=PE(n.children[s],e,o))}),Object.entries(n.children).forEach(([s,o])=>{i[s]===void 0&&(r[s]=o)}),new pt(n.segments,r)}}function p1(n,e,t){let i=0,r=e,s={match:!1,pathIndex:0,commandIndex:0};for(;r<n.segments.length;){if(i>=t.length)return s;let o=n.segments[r],a=t[i];if(Aa(a))break;let c=`${a}`,l=i<t.length-1?t[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!gE(c,l,o))return s;i+=2}else{if(!gE(c,{},o))return s;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function im(n,e,t){let i=n.segments.slice(0,e),r=0;for(;r<t.length;){let s=t[r];if(Aa(s)){let c=m1(s.outlets);return new pt(i,c)}if(r===0&&yu(t[0])){let c=n.segments[e];i.push(new Wr(c.path,mE(t[0]))),r++;continue}let o=Aa(s)?s.outlets[Xe]:`${s}`,a=r<t.length-1?t[r+1]:null;o&&a&&yu(a)?(i.push(new Wr(o,mE(a))),r+=2):(i.push(new Wr(o,{})),r++)}return new pt(i,{})}function m1(n){let e={};return Object.entries(n).forEach(([t,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(e[t]=im(new pt([],{}),0,i))}),e}function mE(n){let e={};return Object.entries(n).forEach(([t,i])=>e[t]=`${i}`),e}function gE(n,e,t){return n==t.path&&ai(e,t.parameters)}var ba="imperative",qt=function(n){return n[n.NavigationStart=0]="NavigationStart",n[n.NavigationEnd=1]="NavigationEnd",n[n.NavigationCancel=2]="NavigationCancel",n[n.NavigationError=3]="NavigationError",n[n.RoutesRecognized=4]="RoutesRecognized",n[n.ResolveStart=5]="ResolveStart",n[n.ResolveEnd=6]="ResolveEnd",n[n.GuardsCheckStart=7]="GuardsCheckStart",n[n.GuardsCheckEnd=8]="GuardsCheckEnd",n[n.RouteConfigLoadStart=9]="RouteConfigLoadStart",n[n.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",n[n.ChildActivationStart=11]="ChildActivationStart",n[n.ChildActivationEnd=12]="ChildActivationEnd",n[n.ActivationStart=13]="ActivationStart",n[n.ActivationEnd=14]="ActivationEnd",n[n.Scroll=15]="Scroll",n[n.NavigationSkipped=16]="NavigationSkipped",n}(qt||{}),Un=class{id;url;constructor(e,t){this.id=e,this.url=t}},Ia=class extends Un{type=qt.NavigationStart;navigationTrigger;restoredState;constructor(e,t,i="imperative",r=null){super(e,t),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},qr=class extends Un{urlAfterRedirects;type=qt.NavigationEnd;constructor(e,t,i){super(e,t),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},Sn=function(n){return n[n.Redirect=0]="Redirect",n[n.SupersededByNewNavigation=1]="SupersededByNewNavigation",n[n.NoDataFromResolver=2]="NoDataFromResolver",n[n.GuardRejected=3]="GuardRejected",n}(Sn||{}),rm=function(n){return n[n.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",n[n.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",n}(rm||{}),Ri=class extends Un{reason;code;type=qt.NavigationCancel;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}},Xr=class extends Un{reason;code;type=qt.NavigationSkipped;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}},Ra=class extends Un{error;target;type=qt.NavigationError;constructor(e,t,i,r){super(e,t),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},xu=class extends Un{urlAfterRedirects;state;type=qt.RoutesRecognized;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},sm=class extends Un{urlAfterRedirects;state;type=qt.GuardsCheckStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},om=class extends Un{urlAfterRedirects;state;shouldActivate;type=qt.GuardsCheckEnd;constructor(e,t,i,r,s){super(e,t),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=s}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},am=class extends Un{urlAfterRedirects;state;type=qt.ResolveStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},cm=class extends Un{urlAfterRedirects;state;type=qt.ResolveEnd;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},lm=class{route;type=qt.RouteConfigLoadStart;constructor(e){this.route=e}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},um=class{route;type=qt.RouteConfigLoadEnd;constructor(e){this.route=e}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},dm=class{snapshot;type=qt.ChildActivationStart;constructor(e){this.snapshot=e}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},fm=class{snapshot;type=qt.ChildActivationEnd;constructor(e){this.snapshot=e}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},hm=class{snapshot;type=qt.ActivationStart;constructor(e){this.snapshot=e}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},pm=class{snapshot;type=qt.ActivationEnd;constructor(e){this.snapshot=e}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Na=class{},po=class{url;navigationBehaviorOptions;constructor(e,t){this.url=e,this.navigationBehaviorOptions=t}};function g1(n,e){return n.providers&&!n._injector&&(n._injector=eu(n.providers,e,`Route: ${n.path}`)),n._injector??e}function Yn(n){return n.outlet||Xe}function v1(n,e){let t=n.filter(i=>Yn(i)===e);return t.push(...n.filter(i=>Yn(i)!==e)),t}function Ua(n){if(!n)return null;if(n.routeConfig?._injector)return n.routeConfig._injector;for(let e=n.parent;e;e=e.parent){let t=e.routeConfig;if(t?._loadedInjector)return t._loadedInjector;if(t?._injector)return t._injector}return null}var mm=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return Ua(this.route?.snapshot)??this.rootInjector}constructor(e){this.rootInjector=e,this.children=new Cu(this.rootInjector)}},Cu=(()=>{class n{rootInjector;contexts=new Map;constructor(t){this.rootInjector=t}onChildOutletCreated(t,i){let r=this.getOrCreateContext(t);r.outlet=i,this.contexts.set(t,r)}onChildOutletDestroyed(t){let i=this.getContext(t);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let t=this.contexts;return this.contexts=new Map,t}onOutletReAttached(t){this.contexts=t}getOrCreateContext(t){let i=this.getContext(t);return i||(i=new mm(this.rootInjector),this.contexts.set(t,i)),i}getContext(t){return this.contexts.get(t)||null}static \u0275fac=function(i){return new(i||n)(et(_n))};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Eu=class{_root;constructor(e){this._root=e}get root(){return this._root.value}parent(e){let t=this.pathFromRoot(e);return t.length>1?t[t.length-2]:null}children(e){let t=gm(e,this._root);return t?t.children.map(i=>i.value):[]}firstChild(e){let t=gm(e,this._root);return t&&t.children.length>0?t.children[0].value:null}siblings(e){let t=vm(e,this._root);return t.length<2?[]:t[t.length-2].children.map(r=>r.value).filter(r=>r!==e)}pathFromRoot(e){return vm(e,this._root).map(t=>t.value)}};function gm(n,e){if(n===e.value)return e;for(let t of e.children){let i=gm(n,t);if(i)return i}return null}function vm(n,e){if(n===e.value)return[e];for(let t of e.children){let i=vm(n,t);if(i.length)return i.unshift(e),i}return[]}var Mn=class{value;children;constructor(e,t){this.value=e,this.children=t}toString(){return`TreeNode(${this.value})`}};function co(n){let e={};return n&&n.children.forEach(t=>e[t.value.outlet]=t),e}var Mu=class extends Eu{snapshot;constructor(e,t){super(e),this.snapshot=t,Cm(this,e)}toString(){return this.snapshot.toString()}};function OE(n){let e=y1(n),t=new Wt([new Wr("",{})]),i=new Wt({}),r=new Wt({}),s=new Wt({}),o=new Wt(""),a=new mo(t,i,s,o,r,Xe,n,e.root);return a.snapshot=e.root,new Mu(new Mn(a,[]),e)}function y1(n){let e={},t={},i={},r="",s=new uo([],e,i,r,t,Xe,n,null,{});return new wu("",new Mn(s,[]))}var mo=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(e,t,i,r,s,o,a,c){this.urlSubject=e,this.paramsSubject=t,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=s,this.outlet=o,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(nt(l=>l[ka]))??ze(void 0),this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=s}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(nt(e=>ho(e))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(nt(e=>ho(e))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function Su(n,e,t="emptyOnly"){let i,{routeConfig:r}=n;return e!==null&&(t==="always"||r?.path===""||!e.component&&!e.routeConfig?.loadComponent)?i={params:be(be({},e.params),n.params),data:be(be({},e.data),n.data),resolve:be(be(be(be({},n.data),e.data),r?.data),n._resolvedData)}:i={params:be({},n.params),data:be({},n.data),resolve:be(be({},n.data),n._resolvedData??{})},r&&FE(r)&&(i.resolve[ka]=r.title),i}var uo=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;get title(){return this.data?.[ka]}constructor(e,t,i,r,s,o,a,c,l){this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=s,this.outlet=o,this.component=a,this.routeConfig=c,this._resolve=l}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=ho(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=ho(this.queryParams),this._queryParamMap}toString(){let e=this.url.map(i=>i.toString()).join("/"),t=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${e}', path:'${t}')`}},wu=class extends Eu{url;constructor(e,t){super(t),this.url=e,Cm(this,t)}toString(){return LE(this._root)}};function Cm(n,e){e.value._routerState=n,e.children.forEach(t=>Cm(n,t))}function LE(n){let e=n.children.length>0?` { ${n.children.map(LE).join(", ")} } `:"";return`${n.value}${e}`}function Kp(n){if(n.snapshot){let e=n.snapshot,t=n._futureSnapshot;n.snapshot=t,ai(e.queryParams,t.queryParams)||n.queryParamsSubject.next(t.queryParams),e.fragment!==t.fragment&&n.fragmentSubject.next(t.fragment),ai(e.params,t.params)||n.paramsSubject.next(t.params),WR(e.url,t.url)||n.urlSubject.next(t.url),ai(e.data,t.data)||n.dataSubject.next(t.data)}else n.snapshot=n._futureSnapshot,n.dataSubject.next(n._futureSnapshot.data)}function ym(n,e){let t=ai(n.params,e.params)&&YR(n.url,e.url),i=!n.parent!=!e.parent;return t&&!i&&(!n.parent||ym(n.parent,e.parent))}function FE(n){return typeof n.title=="string"||n.title===null}var _1=new Ae(""),x1=(()=>{class n{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=Xe;activateEvents=new mn;deactivateEvents=new mn;attachEvents=new mn;detachEvents=new mn;routerOutletData=m0(void 0);parentContexts=Q(Cu);location=Q(Ql);changeDetector=Q(ga);inputBinder=Q(Dm,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(t){if(t.name){let{firstChange:i,previousValue:r}=t.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(t){return this.parentContexts.getContext(t)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let t=this.parentContexts.getContext(this.name);t?.route&&(t.attachRef?this.attach(t.attachRef,t.route):this.activateWith(t.route,t.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new Pe(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new Pe(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new Pe(4012,!1);this.location.detach();let t=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(t.instance),t}attach(t,i){this.activated=t,this._activatedRoute=i,this.location.insert(t.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(t.instance)}deactivate(){if(this.activated){let t=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(t)}}activateWith(t,i){if(this.isActivated)throw new Pe(4013,!1);this._activatedRoute=t;let r=this.location,o=t.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new _m(t,a,r.injector,this.routerOutletData);this.activated=r.createComponent(o,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||n)};static \u0275dir=Op({type:n,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[ip]})}return n})(),_m=class n{route;childContexts;parent;outletData;__ngOutletInjector(e){return new n(this.route,this.childContexts,e,this.outletData)}constructor(e,t,i,r){this.route=e,this.childContexts=t,this.parent=i,this.outletData=r}get(e,t){return e===mo?this.route:e===Cu?this.childContexts:e===_1?this.outletData:this.parent.get(e,t)}},Dm=new Ae("");function E1(n,e,t){let i=Pa(n,e._root,t?t._root:void 0);return new Mu(i,e)}function Pa(n,e,t){if(t&&n.shouldReuseRoute(e.value,t.value.snapshot)){let i=t.value;i._futureSnapshot=e.value;let r=M1(n,e,t);return new Mn(i,r)}else{if(n.shouldAttach(e.value)){let s=n.retrieve(e.value);if(s!==null){let o=s.route;return o.value._futureSnapshot=e.value,o.children=e.children.map(a=>Pa(n,a)),o}}let i=S1(e.value),r=e.children.map(s=>Pa(n,s));return new Mn(i,r)}}function M1(n,e,t){return e.children.map(i=>{for(let r of t.children)if(n.shouldReuseRoute(i.value,r.value.snapshot))return Pa(n,i,r);return Pa(n,i)})}function S1(n){return new mo(new Wt(n.url),new Wt(n.params),new Wt(n.queryParams),new Wt(n.fragment),new Wt(n.data),n.outlet,n.component,n)}var Oa=class{redirectTo;navigationBehaviorOptions;constructor(e,t){this.redirectTo=e,this.navigationBehaviorOptions=t}},kE="ngNavigationCancelingError";function bu(n,e){let{redirectTo:t,navigationBehaviorOptions:i}=Da(e)?{redirectTo:e,navigationBehaviorOptions:void 0}:e,r=UE(!1,Sn.Redirect);return r.url=t,r.navigationBehaviorOptions=i,r}function UE(n,e){let t=new Error(`NavigationCancelingError: ${n||""}`);return t[kE]=!0,t.cancellationCode=e,t}function w1(n){return BE(n)&&Da(n.url)}function BE(n){return!!n&&n[kE]}var b1=(n,e,t,i)=>nt(r=>(new xm(e,r.targetRouterState,r.currentRouterState,t,i).activate(n),r)),xm=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(e,t,i,r,s){this.routeReuseStrategy=e,this.futureState=t,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=s}activate(e){let t=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(t,i,e),Kp(this.futureState.root),this.activateChildRoutes(t,i,e)}deactivateChildRoutes(e,t,i){let r=co(t);e.children.forEach(s=>{let o=s.value.outlet;this.deactivateRoutes(s,r[o],i),delete r[o]}),Object.values(r).forEach(s=>{this.deactivateRouteAndItsChildren(s,i)})}deactivateRoutes(e,t,i){let r=e.value,s=t?t.value:null;if(r===s)if(r.component){let o=i.getContext(r.outlet);o&&this.deactivateChildRoutes(e,t,o.children)}else this.deactivateChildRoutes(e,t,i);else s&&this.deactivateRouteAndItsChildren(t,i)}deactivateRouteAndItsChildren(e,t){e.value.component&&this.routeReuseStrategy.shouldDetach(e.value.snapshot)?this.detachAndStoreRouteSubtree(e,t):this.deactivateRouteAndOutlet(e,t)}detachAndStoreRouteSubtree(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,s=co(e);for(let o of Object.values(s))this.deactivateRouteAndItsChildren(o,r);if(i&&i.outlet){let o=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(e.value.snapshot,{componentRef:o,route:e,contexts:a})}}deactivateRouteAndOutlet(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,s=co(e);for(let o of Object.values(s))this.deactivateRouteAndItsChildren(o,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(e,t,i){let r=co(t);e.children.forEach(s=>{this.activateRoutes(s,r[s.value.outlet],i),this.forwardEvent(new pm(s.value.snapshot))}),e.children.length&&this.forwardEvent(new fm(e.value.snapshot))}activateRoutes(e,t,i){let r=e.value,s=t?t.value:null;if(Kp(r),r===s)if(r.component){let o=i.getOrCreateContext(r.outlet);this.activateChildRoutes(e,t,o.children)}else this.activateChildRoutes(e,t,i);else if(r.component){let o=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),o.children.onOutletReAttached(a.contexts),o.attachRef=a.componentRef,o.route=a.route.value,o.outlet&&o.outlet.attach(a.componentRef,a.route.value),Kp(a.route.value),this.activateChildRoutes(e,null,o.children)}else o.attachRef=null,o.route=r,o.outlet&&o.outlet.activateWith(r,o.injector),this.activateChildRoutes(e,null,o.children)}else this.activateChildRoutes(e,null,i)}},Tu=class{path;route;constructor(e){this.path=e,this.route=this.path[this.path.length-1]}},fo=class{component;route;constructor(e,t){this.component=e,this.route=t}};function T1(n,e,t){let i=n._root,r=e?e._root:null;return Sa(i,r,t,[i.value])}function C1(n){let e=n.routeConfig?n.routeConfig.canActivateChild:null;return!e||e.length===0?null:{node:n,guards:e}}function vo(n,e){let t=Symbol(),i=e.get(n,t);return i===t?typeof n=="function"&&!m_(n)?n:e.get(n):i}function Sa(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let s=co(e);return n.children.forEach(o=>{D1(o,s[o.value.outlet],t,i.concat([o.value]),r),delete s[o.value.outlet]}),Object.entries(s).forEach(([o,a])=>Ta(a,t.getContext(o),r)),r}function D1(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let s=n.value,o=e?e.value:null,a=t?t.getContext(n.value.outlet):null;if(o&&s.routeConfig===o.routeConfig){let c=A1(o,s,s.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new Tu(i)):(s.data=o.data,s._resolvedData=o._resolvedData),s.component?Sa(n,e,a?a.children:null,i,r):Sa(n,e,t,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new fo(a.outlet.component,o))}else o&&Ta(e,a,r),r.canActivateChecks.push(new Tu(i)),s.component?Sa(n,null,a?a.children:null,i,r):Sa(n,null,t,i,r);return r}function A1(n,e,t){if(typeof t=="function")return t(n,e);switch(t){case"pathParamsChange":return!$r(n.url,e.url);case"pathParamsOrQueryParamsChange":return!$r(n.url,e.url)||!ai(n.queryParams,e.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!ym(n,e)||!ai(n.queryParams,e.queryParams);case"paramsChange":default:return!ym(n,e)}}function Ta(n,e,t){let i=co(n),r=n.value;Object.entries(i).forEach(([s,o])=>{r.component?e?Ta(o,e.children.getContext(s),t):Ta(o,null,t):Ta(o,e,t)}),r.component?e&&e.outlet&&e.outlet.isActivated?t.canDeactivateChecks.push(new fo(e.outlet.component,r)):t.canDeactivateChecks.push(new fo(null,r)):t.canDeactivateChecks.push(new fo(null,r))}function Ba(n){return typeof n=="function"}function I1(n){return typeof n=="boolean"}function R1(n){return n&&Ba(n.canLoad)}function N1(n){return n&&Ba(n.canActivate)}function P1(n){return n&&Ba(n.canActivateChild)}function O1(n){return n&&Ba(n.canDeactivate)}function L1(n){return n&&Ba(n.canMatch)}function VE(n){return n instanceof xi||n?.name==="EmptyError"}var pu=Symbol("INITIAL_VALUE");function go(){return Nn(n=>Qc(n.map(e=>e.pipe(Ei(1),Xf(pu)))).pipe(nt(e=>{for(let t of e)if(t!==!0){if(t===pu)return pu;if(t===!1||F1(t))return t}return!0}),Rn(e=>e!==pu),Ei(1)))}function F1(n){return Da(n)||n instanceof Oa}function k1(n,e){return Lt(t=>{let{targetSnapshot:i,currentSnapshot:r,guards:{canActivateChecks:s,canDeactivateChecks:o}}=t;return o.length===0&&s.length===0?ze(Mt(be({},t),{guardsResult:!0})):U1(o,i,r,n).pipe(Lt(a=>a&&I1(a)?B1(i,s,n,e):ze(a)),nt(a=>Mt(be({},t),{guardsResult:a})))})}function U1(n,e,t,i){return Nt(n).pipe(Lt(r=>j1(r.component,r.route,t,e,i)),Mi(r=>r!==!0,!0))}function B1(n,e,t,i){return Nt(e).pipe(Rr(r=>Ns(z1(r.route.parent,i),V1(r.route,i),G1(n,r.path,t),H1(n,r.route,t))),Mi(r=>r!==!0,!0))}function V1(n,e){return n!==null&&e&&e(new hm(n)),ze(!0)}function z1(n,e){return n!==null&&e&&e(new dm(n)),ze(!0)}function H1(n,e,t){let i=e.routeConfig?e.routeConfig.canActivate:null;if(!i||i.length===0)return ze(!0);let r=i.map(s=>el(()=>{let o=Ua(e)??t,a=vo(s,o),c=N1(a)?a.canActivate(e,n):Fn(o,()=>a(e,n));return cr(c).pipe(Mi())}));return ze(r).pipe(go())}function G1(n,e,t){let i=e[e.length-1],s=e.slice(0,e.length-1).reverse().map(o=>C1(o)).filter(o=>o!==null).map(o=>el(()=>{let a=o.guards.map(c=>{let l=Ua(o.node)??t,u=vo(c,l),d=P1(u)?u.canActivateChild(i,n):Fn(l,()=>u(i,n));return cr(d).pipe(Mi())});return ze(a).pipe(go())}));return ze(s).pipe(go())}function j1(n,e,t,i,r){let s=e&&e.routeConfig?e.routeConfig.canDeactivate:null;if(!s||s.length===0)return ze(!0);let o=s.map(a=>{let c=Ua(e)??r,l=vo(a,c),u=O1(l)?l.canDeactivate(n,e,t,i):Fn(c,()=>l(n,e,t,i));return cr(u).pipe(Mi())});return ze(o).pipe(go())}function W1(n,e,t,i){let r=e.canLoad;if(r===void 0||r.length===0)return ze(!0);let s=r.map(o=>{let a=vo(o,n),c=R1(a)?a.canLoad(e,t):Fn(n,()=>a(e,t));return cr(c)});return ze(s).pipe(go(),zE(i))}function zE(n){return Vf(Ft(e=>{if(typeof e!="boolean")throw bu(n,e)}),nt(e=>e===!0))}function $1(n,e,t,i){let r=e.canMatch;if(!r||r.length===0)return ze(!0);let s=r.map(o=>{let a=vo(o,n),c=L1(a)?a.canMatch(e,t):Fn(n,()=>a(e,t));return cr(c)});return ze(s).pipe(go(),zE(i))}var La=class{segmentGroup;constructor(e){this.segmentGroup=e||null}},Fa=class extends Error{urlTree;constructor(e){super(),this.urlTree=e}};function ao(n){return Rs(new La(n))}function q1(n){return Rs(new Pe(4e3,!1))}function X1(n){return Rs(UE(!1,Sn.GuardRejected))}var Em=class{urlSerializer;urlTree;constructor(e,t){this.urlSerializer=e,this.urlTree=t}lineralizeSegments(e,t){let i=[],r=t.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return ze(i);if(r.numberOfChildren>1||!r.children[Xe])return q1(`${e.redirectTo}`);r=r.children[Xe]}}applyRedirectCommands(e,t,i,r,s){if(typeof t!="string"){let a=t,{queryParams:c,fragment:l,routeConfig:u,url:d,outlet:f,params:h,data:g,title:v}=r,m=Fn(s,()=>a({params:h,data:g,queryParams:c,fragment:l,routeConfig:u,url:d,outlet:f,title:v}));if(m instanceof Ni)throw new Fa(m);t=m}let o=this.applyRedirectCreateUrlTree(t,this.urlSerializer.parse(t),e,i);if(t[0]==="/")throw new Fa(o);return o}applyRedirectCreateUrlTree(e,t,i,r){let s=this.createSegmentGroup(e,t.root,i,r);return new Ni(s,this.createQueryParams(t.queryParams,this.urlTree.queryParams),t.fragment)}createQueryParams(e,t){let i={};return Object.entries(e).forEach(([r,s])=>{if(typeof s=="string"&&s[0]===":"){let a=s.substring(1);i[r]=t[a]}else i[r]=s}),i}createSegmentGroup(e,t,i,r){let s=this.createSegments(e,t.segments,i,r),o={};return Object.entries(t.children).forEach(([a,c])=>{o[a]=this.createSegmentGroup(e,c,i,r)}),new pt(s,o)}createSegments(e,t,i,r){return t.map(s=>s.path[0]===":"?this.findPosParam(e,s,r):this.findOrReturn(s,i))}findPosParam(e,t,i){let r=i[t.path.substring(1)];if(!r)throw new Pe(4001,!1);return r}findOrReturn(e,t){let i=0;for(let r of t){if(r.path===e.path)return t.splice(i),r;i++}return e}},Mm={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function Y1(n,e,t,i,r){let s=HE(n,e,t);return s.matched?(i=g1(e,i),$1(i,e,t,r).pipe(nt(o=>o===!0?s:be({},Mm)))):ze(s)}function HE(n,e,t){if(e.path==="**")return Z1(t);if(e.path==="")return e.pathMatch==="full"&&(n.hasChildren()||t.length>0)?be({},Mm):{matched:!0,consumedSegments:[],remainingSegments:t,parameters:{},positionalParamSegments:{}};let r=(e.matcher||jR)(t,n,e);if(!r)return be({},Mm);let s={};Object.entries(r.posParams??{}).forEach(([a,c])=>{s[a]=c.path});let o=r.consumed.length>0?be(be({},s),r.consumed[r.consumed.length-1].parameters):s;return{matched:!0,consumedSegments:r.consumed,remainingSegments:t.slice(r.consumed.length),parameters:o,positionalParamSegments:r.posParams??{}}}function Z1(n){return{matched:!0,parameters:n.length>0?EE(n).parameters:{},consumedSegments:n,remainingSegments:[],positionalParamSegments:{}}}function vE(n,e,t,i){return t.length>0&&Q1(n,t,i)?{segmentGroup:new pt(e,J1(i,new pt(t,n.children))),slicedSegments:[]}:t.length===0&&eN(n,t,i)?{segmentGroup:new pt(n.segments,K1(n,t,i,n.children)),slicedSegments:t}:{segmentGroup:new pt(n.segments,n.children),slicedSegments:t}}function K1(n,e,t,i){let r={};for(let s of t)if(Du(n,e,s)&&!i[Yn(s)]){let o=new pt([],{});r[Yn(s)]=o}return be(be({},i),r)}function J1(n,e){let t={};t[Xe]=e;for(let i of n)if(i.path===""&&Yn(i)!==Xe){let r=new pt([],{});t[Yn(i)]=r}return t}function Q1(n,e,t){return t.some(i=>Du(n,e,i)&&Yn(i)!==Xe)}function eN(n,e,t){return t.some(i=>Du(n,e,i))}function Du(n,e,t){return(n.hasChildren()||e.length>0)&&t.pathMatch==="full"?!1:t.path===""}function tN(n,e,t){return e.length===0&&!n.children[t]}var Sm=class{};function nN(n,e,t,i,r,s,o="emptyOnly"){return new wm(n,e,t,i,r,o,s).recognize()}var iN=31,wm=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(e,t,i,r,s,o,a){this.injector=e,this.configLoader=t,this.rootComponentType=i,this.config=r,this.urlTree=s,this.paramsInheritanceStrategy=o,this.urlSerializer=a,this.applyRedirects=new Em(this.urlSerializer,this.urlTree)}noMatchError(e){return new Pe(4002,`'${e.segmentGroup}'`)}recognize(){let e=vE(this.urlTree.root,[],[],this.config).segmentGroup;return this.match(e).pipe(nt(({children:t,rootSnapshot:i})=>{let r=new Mn(i,t),s=new wu("",r),o=l1(i,[],this.urlTree.queryParams,this.urlTree.fragment);return o.queryParams=this.urlTree.queryParams,s.url=this.urlSerializer.serialize(o),{state:s,tree:o}}))}match(e){let t=new uo([],Object.freeze({}),Object.freeze(be({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),Xe,this.rootComponentType,null,{});return this.processSegmentGroup(this.injector,this.config,e,Xe,t).pipe(nt(i=>({children:i,rootSnapshot:t})),Xi(i=>{if(i instanceof Fa)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof La?this.noMatchError(i):i}))}processSegmentGroup(e,t,i,r,s){return i.segments.length===0&&i.hasChildren()?this.processChildren(e,t,i,s):this.processSegment(e,t,i,i.segments,r,!0,s).pipe(nt(o=>o instanceof Mn?[o]:[]))}processChildren(e,t,i,r){let s=[];for(let o of Object.keys(i.children))o==="primary"?s.unshift(o):s.push(o);return Nt(s).pipe(Rr(o=>{let a=i.children[o],c=v1(t,o);return this.processSegmentGroup(e,c,a,o,r)}),qf((o,a)=>(o.push(...a),o)),Yi(null),$f(),Lt(o=>{if(o===null)return ao(i);let a=GE(o);return rN(a),ze(a)}))}processSegment(e,t,i,r,s,o,a){return Nt(t).pipe(Rr(c=>this.processSegmentAgainstRoute(c._injector??e,t,c,i,r,s,o,a).pipe(Xi(l=>{if(l instanceof La)return ze(null);throw l}))),Mi(c=>!!c),Xi(c=>{if(VE(c))return tN(i,r,s)?ze(new Sm):ao(i);throw c}))}processSegmentAgainstRoute(e,t,i,r,s,o,a,c){return Yn(i)!==o&&(o===Xe||!Du(r,s,i))?ao(r):i.redirectTo===void 0?this.matchSegmentAgainstRoute(e,r,i,s,o,c):this.allowRedirects&&a?this.expandSegmentAgainstRouteUsingRedirect(e,r,t,i,s,o,c):ao(r)}expandSegmentAgainstRouteUsingRedirect(e,t,i,r,s,o,a){let{matched:c,parameters:l,consumedSegments:u,positionalParamSegments:d,remainingSegments:f}=HE(t,r,s);if(!c)return ao(t);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>iN&&(this.allowRedirects=!1));let h=new uo(s,l,Object.freeze(be({},this.urlTree.queryParams)),this.urlTree.fragment,yE(r),Yn(r),r.component??r._loadedComponent??null,r,_E(r)),g=Su(h,a,this.paramsInheritanceStrategy);h.params=Object.freeze(g.params),h.data=Object.freeze(g.data);let v=this.applyRedirects.applyRedirectCommands(u,r.redirectTo,d,h,e);return this.applyRedirects.lineralizeSegments(r,v).pipe(Lt(m=>this.processSegment(e,i,t,m.concat(f),o,!1,a)))}matchSegmentAgainstRoute(e,t,i,r,s,o){let a=Y1(t,i,r,e,this.urlSerializer);return i.path==="**"&&(t.children={}),a.pipe(Nn(c=>c.matched?(e=i._injector??e,this.getChildConfig(e,i,r).pipe(Nn(({routes:l})=>{let u=i._loadedInjector??e,{parameters:d,consumedSegments:f,remainingSegments:h}=c,g=new uo(f,d,Object.freeze(be({},this.urlTree.queryParams)),this.urlTree.fragment,yE(i),Yn(i),i.component??i._loadedComponent??null,i,_E(i)),v=Su(g,o,this.paramsInheritanceStrategy);g.params=Object.freeze(v.params),g.data=Object.freeze(v.data);let{segmentGroup:m,slicedSegments:p}=vE(t,f,h,l);if(p.length===0&&m.hasChildren())return this.processChildren(u,l,m,g).pipe(nt(M=>new Mn(g,M)));if(l.length===0&&p.length===0)return ze(new Mn(g,[]));let T=Yn(i)===s;return this.processSegment(u,l,m,p,T?Xe:s,!0,g).pipe(nt(M=>new Mn(g,M instanceof Mn?[M]:[])))}))):ao(t)))}getChildConfig(e,t,i){return t.children?ze({routes:t.children,injector:e}):t.loadChildren?t._loadedRoutes!==void 0?ze({routes:t._loadedRoutes,injector:t._loadedInjector}):W1(e,t,i,this.urlSerializer).pipe(Lt(r=>r?this.configLoader.loadChildren(e,t).pipe(Ft(s=>{t._loadedRoutes=s.routes,t._loadedInjector=s.injector})):X1(t))):ze({routes:[],injector:e})}};function rN(n){n.sort((e,t)=>e.value.outlet===Xe?-1:t.value.outlet===Xe?1:e.value.outlet.localeCompare(t.value.outlet))}function sN(n){let e=n.value.routeConfig;return e&&e.path===""}function GE(n){let e=[],t=new Set;for(let i of n){if(!sN(i)){e.push(i);continue}let r=e.find(s=>i.value.routeConfig===s.value.routeConfig);r!==void 0?(r.children.push(...i.children),t.add(r)):e.push(i)}for(let i of t){let r=GE(i.children);e.push(new Mn(i.value,r))}return e.filter(i=>!t.has(i))}function yE(n){return n.data||{}}function _E(n){return n.resolve||{}}function oN(n,e,t,i,r,s){return Lt(o=>nN(n,e,t,i,o.extractedUrl,r,s).pipe(nt(({state:a,tree:c})=>Mt(be({},o),{targetSnapshot:a,urlAfterRedirects:c}))))}function aN(n,e){return Lt(t=>{let{targetSnapshot:i,guards:{canActivateChecks:r}}=t;if(!r.length)return ze(t);let s=new Set(r.map(c=>c.route)),o=new Set;for(let c of s)if(!o.has(c))for(let l of jE(c))o.add(l);let a=0;return Nt(o).pipe(Rr(c=>s.has(c)?cN(c,i,n,e):(c.data=Su(c,c.parent,n).resolve,ze(void 0))),Ft(()=>a++),Os(1),Lt(c=>a===o.size?ze(t):an))})}function jE(n){let e=n.children.map(t=>jE(t)).flat();return[n,...e]}function cN(n,e,t,i){let r=n.routeConfig,s=n._resolve;return r?.title!==void 0&&!FE(r)&&(s[ka]=r.title),lN(s,n,e,i).pipe(nt(o=>(n._resolvedData=o,n.data=Su(n,n.parent,t).resolve,null)))}function lN(n,e,t,i){let r=em(n);if(r.length===0)return ze({});let s={};return Nt(r).pipe(Lt(o=>uN(n[o],e,t,i).pipe(Mi(),Ft(a=>{if(a instanceof Oa)throw bu(new Ca,a);s[o]=a}))),Os(1),nt(()=>s),Xi(o=>VE(o)?an:Rs(o)))}function uN(n,e,t,i){let r=Ua(e)??i,s=vo(n,r),o=s.resolve?s.resolve(e,t):Fn(r,()=>s(e,t));return cr(o)}function Jp(n){return Nn(e=>{let t=n(e);return t?Nt(t).pipe(nt(()=>e)):ze(e)})}var WE=(()=>{class n{buildTitle(t){let i,r=t.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(s=>s.outlet===Xe);return i}getResolvedTitleForRoute(t){return t.data[ka]}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:()=>Q(dN),providedIn:"root"})}return n})(),dN=(()=>{class n extends WE{title;constructor(t){super(),this.title=t}updateTitle(t){let i=this.buildTitle(t);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||n)(et(uE))};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Am=new Ae("",{providedIn:"root",factory:()=>({})}),fN=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=so({type:n,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&ar(0,"router-outlet")},dependencies:[x1],encapsulation:2})}return n})();function Im(n){let e=n.children&&n.children.map(Im),t=e?Mt(be({},n),{children:e}):be({},n);return!t.component&&!t.loadComponent&&(e||t.loadChildren)&&t.outlet&&t.outlet!==Xe&&(t.component=fN),t}var Rm=new Ae(""),hN=(()=>{class n{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=Q(Fp);loadComponent(t){if(this.componentLoaders.get(t))return this.componentLoaders.get(t);if(t._loadedComponent)return ze(t._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(t);let i=cr(t.loadComponent()).pipe(nt($E),Ft(s=>{this.onLoadEndListener&&this.onLoadEndListener(t),t._loadedComponent=s}),Ps(()=>{this.componentLoaders.delete(t)})),r=new Is(i,()=>new Yt).pipe(As());return this.componentLoaders.set(t,r),r}loadChildren(t,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return ze({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let s=pN(i,this.compiler,t,this.onLoadEndListener).pipe(Ps(()=>{this.childrenLoaders.delete(i)})),o=new Is(s,()=>new Yt).pipe(As());return this.childrenLoaders.set(i,o),o}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function pN(n,e,t,i){return cr(n.loadChildren()).pipe(nt($E),Lt(r=>r instanceof la||Array.isArray(r)?ze(r):Nt(e.compileModuleAsync(r))),nt(r=>{i&&i(n);let s,o,a=!1;return Array.isArray(r)?(o=r,a=!0):(s=r.create(t).injector,o=s.get(Rm,[],{optional:!0,self:!0}).flat()),{routes:o.map(Im),injector:s}}))}function mN(n){return n&&typeof n=="object"&&"default"in n}function $E(n){return mN(n)?n.default:n}var Nm=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:()=>Q(gN),providedIn:"root"})}return n})(),gN=(()=>{class n{shouldProcessUrl(t){return!0}extract(t){return t}merge(t,i){return t}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),vN=new Ae("");var yN=new Ae(""),_N=(()=>{class n{currentNavigation=null;currentTransition=null;lastSuccessfulNavigation=null;events=new Yt;transitionAbortSubject=new Yt;configLoader=Q(hN);environmentInjector=Q(_n);destroyRef=Q(fa);urlSerializer=Q(Tm);rootContexts=Q(Cu);location=Q(ya);inputBindingEnabled=Q(Dm,{optional:!0})!==null;titleStrategy=Q(WE);options=Q(Am,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=Q(Nm);createViewTransition=Q(vN,{optional:!0});navigationErrorHandler=Q(yN,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>ze(void 0);rootComponentType=null;destroyed=!1;constructor(){let t=r=>this.events.next(new lm(r)),i=r=>this.events.next(new um(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=t,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(t){let i=++this.navigationId;this.transitions?.next(Mt(be(be({},this.transitions.value),t),{id:i}))}setupNavigations(t,i,r){return this.transitions=new Wt({id:0,currentUrlTree:i,currentRawUrl:i,extractedUrl:this.urlHandlingStrategy.extract(i),urlAfterRedirects:this.urlHandlingStrategy.extract(i),rawUrl:i,extras:{},resolve:()=>{},reject:()=>{},promise:Promise.resolve(!0),source:ba,restoredState:null,currentSnapshot:r.snapshot,targetSnapshot:null,currentRouterState:r,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null}),this.transitions.pipe(Rn(s=>s.id!==0),nt(s=>Mt(be({},s),{extractedUrl:this.urlHandlingStrategy.extract(s.rawUrl)})),Nn(s=>{let o=!1,a=!1;return ze(s).pipe(Nn(c=>{if(this.navigationId>s.id)return this.cancelNavigationTransition(s,"",Sn.SupersededByNewNavigation),an;this.currentTransition=s,this.currentNavigation={id:c.id,initialUrl:c.rawUrl,extractedUrl:c.extractedUrl,targetBrowserUrl:typeof c.extras.browserUrl=="string"?this.urlSerializer.parse(c.extras.browserUrl):c.extras.browserUrl,trigger:c.source,extras:c.extras,previousNavigation:this.lastSuccessfulNavigation?Mt(be({},this.lastSuccessfulNavigation),{previousNavigation:null}):null};let l=!t.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=c.extras.onSameUrlNavigation??t.onSameUrlNavigation;if(!l&&u!=="reload"){let d="";return this.events.next(new Xr(c.id,this.urlSerializer.serialize(c.rawUrl),d,rm.IgnoredSameUrlNavigation)),c.resolve(!1),an}if(this.urlHandlingStrategy.shouldProcessUrl(c.rawUrl))return ze(c).pipe(Nn(d=>{let f=this.transitions?.getValue();return this.events.next(new Ia(d.id,this.urlSerializer.serialize(d.extractedUrl),d.source,d.restoredState)),f!==this.transitions?.getValue()?an:Promise.resolve(d)}),oN(this.environmentInjector,this.configLoader,this.rootComponentType,t.config,this.urlSerializer,this.paramsInheritanceStrategy),Ft(d=>{s.targetSnapshot=d.targetSnapshot,s.urlAfterRedirects=d.urlAfterRedirects,this.currentNavigation=Mt(be({},this.currentNavigation),{finalUrl:d.urlAfterRedirects});let f=new xu(d.id,this.urlSerializer.serialize(d.extractedUrl),this.urlSerializer.serialize(d.urlAfterRedirects),d.targetSnapshot);this.events.next(f)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(c.currentRawUrl)){let{id:d,extractedUrl:f,source:h,restoredState:g,extras:v}=c,m=new Ia(d,this.urlSerializer.serialize(f),h,g);this.events.next(m);let p=OE(this.rootComponentType).snapshot;return this.currentTransition=s=Mt(be({},c),{targetSnapshot:p,urlAfterRedirects:f,extras:Mt(be({},v),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.finalUrl=f,ze(s)}else{let d="";return this.events.next(new Xr(c.id,this.urlSerializer.serialize(c.extractedUrl),d,rm.IgnoredByUrlHandlingStrategy)),c.resolve(!1),an}}),Ft(c=>{let l=new sm(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(l)}),nt(c=>(this.currentTransition=s=Mt(be({},c),{guards:T1(c.targetSnapshot,c.currentSnapshot,this.rootContexts)}),s)),k1(this.environmentInjector,c=>this.events.next(c)),Ft(c=>{if(s.guardsResult=c.guardsResult,c.guardsResult&&typeof c.guardsResult!="boolean")throw bu(this.urlSerializer,c.guardsResult);let l=new om(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot,!!c.guardsResult);this.events.next(l)}),Rn(c=>c.guardsResult?!0:(this.cancelNavigationTransition(c,"",Sn.GuardRejected),!1)),Jp(c=>{if(c.guards.canActivateChecks.length)return ze(c).pipe(Ft(l=>{let u=new am(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);this.events.next(u)}),Nn(l=>{let u=!1;return ze(l).pipe(aN(this.paramsInheritanceStrategy,this.environmentInjector),Ft({next:()=>u=!0,complete:()=>{u||this.cancelNavigationTransition(l,"",Sn.NoDataFromResolver)}}))}),Ft(l=>{let u=new cm(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);this.events.next(u)}))}),Jp(c=>{let l=u=>{let d=[];u.routeConfig?.loadComponent&&!u.routeConfig._loadedComponent&&d.push(this.configLoader.loadComponent(u.routeConfig).pipe(Ft(f=>{u.component=f}),nt(()=>{})));for(let f of u.children)d.push(...l(f));return d};return Qc(l(c.targetSnapshot.root)).pipe(Yi(null),Ei(1))}),Jp(()=>this.afterPreactivation()),Nn(()=>{let{currentSnapshot:c,targetSnapshot:l}=s,u=this.createViewTransition?.(this.environmentInjector,c.root,l.root);return u?Nt(u).pipe(nt(()=>s)):ze(s)}),nt(c=>{let l=E1(t.routeReuseStrategy,c.targetSnapshot,c.currentRouterState);return this.currentTransition=s=Mt(be({},c),{targetRouterState:l}),this.currentNavigation.targetRouterState=l,s}),Ft(()=>{this.events.next(new Na)}),b1(this.rootContexts,t.routeReuseStrategy,c=>this.events.next(c),this.inputBindingEnabled),Ei(1),Ft({next:c=>{o=!0,this.lastSuccessfulNavigation=this.currentNavigation,this.events.next(new qr(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects))),this.titleStrategy?.updateTitle(c.targetRouterState.snapshot),c.resolve(!0)},complete:()=>{o=!0}}),Yf(this.transitionAbortSubject.pipe(Ft(c=>{throw c}))),Ps(()=>{!o&&!a&&this.cancelNavigationTransition(s,"",Sn.SupersededByNewNavigation),this.currentTransition?.id===s.id&&(this.currentNavigation=null,this.currentTransition=null)}),Xi(c=>{if(this.destroyed)return s.resolve(!1),an;if(a=!0,BE(c))this.events.next(new Ri(s.id,this.urlSerializer.serialize(s.extractedUrl),c.message,c.cancellationCode)),w1(c)?this.events.next(new po(c.url,c.navigationBehaviorOptions)):s.resolve(!1);else{let l=new Ra(s.id,this.urlSerializer.serialize(s.extractedUrl),c,s.targetSnapshot??void 0);try{let u=Fn(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(u instanceof Oa){let{message:d,cancellationCode:f}=bu(this.urlSerializer,u);this.events.next(new Ri(s.id,this.urlSerializer.serialize(s.extractedUrl),d,f)),this.events.next(new po(u.redirectTo,u.navigationBehaviorOptions))}else throw this.events.next(l),c}catch(u){this.options.resolveNavigationPromiseOnError?s.resolve(!1):s.reject(u)}}return an}))}))}cancelNavigationTransition(t,i,r){let s=new Ri(t.id,this.urlSerializer.serialize(t.extractedUrl),i,r);this.events.next(s),t.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let t=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=this.currentNavigation?.targetBrowserUrl??this.currentNavigation?.extractedUrl;return t.toString()!==i?.toString()&&!this.currentNavigation?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function xN(n){return n!==ba}var EN=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:()=>Q(MN),providedIn:"root"})}return n})(),bm=class{shouldDetach(e){return!1}store(e,t){}shouldAttach(e){return!1}retrieve(e){return null}shouldReuseRoute(e,t){return e.routeConfig===t.routeConfig}},MN=(()=>{class n extends bm{static \u0275fac=(()=>{let t;return function(r){return(t||(t=dp(n)))(r||n)}})();static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),qE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:()=>Q(SN),providedIn:"root"})}return n})(),SN=(()=>{class n extends qE{location=Q(ya);urlSerializer=Q(Tm);options=Q(Am,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";urlHandlingStrategy=Q(Nm);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new Ni;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}currentPageId=0;lastSuccessfulId=-1;restoredState(){return this.location.getState()}get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}routerState=OE(null);getRouterState(){return this.routerState}stateMemento=this.createStateMemento();createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}registerNonRouterCurrentEntryChangeListener(t){return this.location.subscribe(i=>{i.type==="popstate"&&t(i.url,i.state)})}handleRouterEvent(t,i){if(t instanceof Ia)this.stateMemento=this.createStateMemento();else if(t instanceof Xr)this.rawUrlTree=i.initialUrl;else if(t instanceof xu){if(this.urlUpdateStrategy==="eager"&&!i.extras.skipLocationChange){let r=this.urlHandlingStrategy.merge(i.finalUrl,i.initialUrl);this.setBrowserUrl(i.targetBrowserUrl??r,i)}}else t instanceof Na?(this.currentUrlTree=i.finalUrl,this.rawUrlTree=this.urlHandlingStrategy.merge(i.finalUrl,i.initialUrl),this.routerState=i.targetRouterState,this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(i.targetBrowserUrl??this.rawUrlTree,i)):t instanceof Ri&&(t.code===Sn.GuardRejected||t.code===Sn.NoDataFromResolver)?this.restoreHistory(i):t instanceof Ra?this.restoreHistory(i,!0):t instanceof qr&&(this.lastSuccessfulId=t.id,this.currentPageId=this.browserPageId)}setBrowserUrl(t,i){let r=t instanceof Ni?this.urlSerializer.serialize(t):t;if(this.location.isCurrentPathEqualTo(r)||i.extras.replaceUrl){let s=this.browserPageId,o=be(be({},i.extras.state),this.generateNgRouterState(i.id,s));this.location.replaceState(r,"",o)}else{let s=be(be({},i.extras.state),this.generateNgRouterState(i.id,this.browserPageId+1));this.location.go(r,"",s)}}restoreHistory(t,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,s=this.currentPageId-r;s!==0?this.location.historyGo(s):this.currentUrlTree===t.finalUrl&&s===0&&(this.resetState(t),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetState(t),this.resetUrlToCurrentUrlTree())}resetState(t){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,t.finalUrl??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.rawUrlTree),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(t,i){return this.canceledNavigationResolution==="computed"?{navigationId:t,\u0275routerPageId:i}:{navigationId:t}}static \u0275fac=(()=>{let t;return function(r){return(t||(t=dp(n)))(r||n)}})();static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function wN(n,e){n.events.pipe(Rn(t=>t instanceof qr||t instanceof Ri||t instanceof Ra||t instanceof Xr),nt(t=>t instanceof qr||t instanceof Xr?0:(t instanceof Ri?t.code===Sn.Redirect||t.code===Sn.SupersededByNewNavigation:!1)?2:1),Rn(t=>t!==2),Ei(1)).subscribe(()=>{e()})}var bN={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},TN={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"},XE=(()=>{class n{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=Q(Lp);stateManager=Q(qE);options=Q(Am,{optional:!0})||{};pendingTasks=Q(Ai);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=Q(_N);urlSerializer=Q(Tm);location=Q(ya);urlHandlingStrategy=Q(Nm);_events=new Yt;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=Q(EN);onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=Q(Rm,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!Q(Dm,{optional:!0});constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this,this.currentUrlTree,this.routerState).subscribe({error:t=>{this.console.warn(t)}}),this.subscribeToNavigationEvents()}eventsSubscription=new Rt;subscribeToNavigationEvents(){let t=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,s=this.navigationTransitions.currentNavigation;if(r!==null&&s!==null){if(this.stateManager.handleRouterEvent(i,s),i instanceof Ri&&i.code!==Sn.Redirect&&i.code!==Sn.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof qr)this.navigated=!0;else if(i instanceof po){let o=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=be({browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||xN(r.source)},o);this.scheduleNavigation(a,ba,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}DN(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortSubject.next(r)}});this.eventsSubscription.add(t)}resetRootComponentType(t){this.routerState.root.component=t,this.navigationTransitions.rootComponentType=t}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),ba,this.stateManager.restoredState())}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((t,i)=>{setTimeout(()=>{this.navigateToSyncWithBrowser(t,"popstate",i)},0)})}navigateToSyncWithBrowser(t,i,r){let s={replaceUrl:!0},o=r?.navigationId?r:null;if(r){let c=be({},r);delete c.navigationId,delete c.\u0275routerPageId,Object.keys(c).length!==0&&(s.state=c)}let a=this.parseUrl(t);this.scheduleNavigation(a,i,o,s)}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return this.navigationTransitions.currentNavigation}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(t){this.config=t.map(Im),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription&&(this.nonRouterCurrentEntryChangeSubscription.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0),this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(t,i={}){let{relativeTo:r,queryParams:s,fragment:o,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:o,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=be(be({},this.currentUrlTree.queryParams),s);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=s||null}u!==null&&(u=this.removeEmptyProps(u));let d;try{let f=r?r.snapshot:this.routerState.snapshot.root;d=IE(f)}catch{(typeof t[0]!="string"||t[0][0]!=="/")&&(t=[]),d=this.currentUrlTree.root}return RE(d,t,u,l??null)}navigateByUrl(t,i={skipLocationChange:!1}){let r=Da(t)?t:this.parseUrl(t),s=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(s,ba,null,i)}navigate(t,i={skipLocationChange:!1}){return CN(t),this.navigateByUrl(this.createUrlTree(t,i),i)}serializeUrl(t){return this.urlSerializer.serialize(t)}parseUrl(t){try{return this.urlSerializer.parse(t)}catch{return this.urlSerializer.parse("/")}}isActive(t,i){let r;if(i===!0?r=be({},bN):i===!1?r=be({},TN):r=i,Da(t))return hE(this.currentUrlTree,t,r);let s=this.parseUrl(t);return hE(this.currentUrlTree,s,r)}removeEmptyProps(t){return Object.entries(t).reduce((i,[r,s])=>(s!=null&&(i[r]=s),i),{})}scheduleNavigation(t,i,r,s,o){if(this.disposed)return Promise.resolve(!1);let a,c,l;o?(a=o.resolve,c=o.reject,l=o.promise):l=new Promise((d,f)=>{a=d,c=f});let u=this.pendingTasks.add();return wN(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:t,extras:s,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(d=>Promise.reject(d))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=ke({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function CN(n){for(let e=0;e<n.length;e++)if(n[e]==null)throw new Pe(4008,!1)}function DN(n){return!(n instanceof Na)&&!(n instanceof po)}var AN=new Ae("");function YE(n,...e){return Br([{provide:Rm,multi:!0,useValue:n},[],{provide:mo,useFactory:IN,deps:[XE]},{provide:Gr,multi:!0,useFactory:RN},e.map(t=>t.\u0275providers)])}function IN(n){return n.routerState.root}function RN(){let n=Q(En);return e=>{let t=n.get(ln);if(e!==t.components[0])return;let i=n.get(XE),r=n.get(NN);n.get(PN)===1&&i.initialNavigation(),n.get(ON,null,Ke.Optional)?.setUpPreloading(),n.get(AN,null,Ke.Optional)?.init(),i.resetRootComponentType(t.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var NN=new Ae("",{factory:()=>new Yt}),PN=new Ae("",{providedIn:"root",factory:()=>1});var ON=new Ae("");var ZE=[];var KE={providers:[Ox({eventCoalescing:!0}),YE(ZE),fE(dE())]};var Cd="173";var EM=0,yg=1,MM=2;var _g=1,SM=2,hi=3,Vi=0,sn=1,pi=2,Gi=0,rs=1,xg=2,Eg=3,Mg=4,wM=5,vr=100,bM=101,TM=102,CM=103,DM=104,AM=200,IM=201,RM=202,NM=203,Ku=204,Ju=205,PM=206,OM=207,LM=208,FM=209,kM=210,UM=211,BM=212,VM=213,zM=214,Dd=0,Ad=1,Id=2,ss=3,Rd=4,Nd=5,Pd=6,Od=7,Ld=0,HM=1,GM=2,ji=0,jM=1,WM=2,$M=3,qM=4,XM=5,YM=6,ZM=7;var sg=300,hs=301,ps=302,Fd=303,kd=304,Ec=306,Qu=1e3,gr=1001,ed=1002,Vn=1003,KM=1004;var Mc=1005;var Qn=1006,Ud=1007;var wr=1008;var mi=1009,Sg=1010,wg=1011,Vo=1012,Bd=1013,br=1014,gi=1015,zo=1016,Vd=1017,zd=1018,ms=1020,bg=35902,Tg=1021,Cg=1022,Hn=1023,Dg=1024,Ag=1025,ts=1026,os=1027,Ig=1028,Hd=1029,Rg=1030,Gd=1031;var jd=1033,Sc=33776,wc=33777,bc=33778,Tc=33779,Wd=35840,$d=35841,qd=35842,Xd=35843,Yd=36196,Zd=37492,Kd=37496,Jd=37808,Qd=37809,ef=37810,tf=37811,nf=37812,rf=37813,sf=37814,of=37815,af=37816,cf=37817,lf=37818,uf=37819,df=37820,ff=37821,Cc=36492,hf=36494,pf=36495,Ng=36283,mf=36284,gf=36285,vf=36286;var qa=2300,td=2301,Zu=2302,og=2400,ag=2401,cg=2402;var JM=3200,QM=3201;var Pg=0,eS=1,Wi="",Tn="srgb",as="srgb-linear",Xa="linear",_t="srgb";var Qr=7680;var lg=519,tS=512,nS=513,iS=514,Og=515,rS=516,sS=517,oS=518,aS=519,ug=35044;var Lg="300 es",li=2e3,Ya=2001;var zi=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},Kt=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"];var Pm=Math.PI/180,nd=180/Math.PI;function Ho(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(Kt[n&255]+Kt[n>>8&255]+Kt[n>>16&255]+Kt[n>>24&255]+"-"+Kt[e&255]+Kt[e>>8&255]+"-"+Kt[e>>16&15|64]+Kt[e>>24&255]+"-"+Kt[t&63|128]+Kt[t>>8&255]+"-"+Kt[t>>16&255]+Kt[t>>24&255]+Kt[i&255]+Kt[i>>8&255]+Kt[i>>16&255]+Kt[i>>24&255]).toLowerCase()}function Je(n,e,t){return Math.max(e,Math.min(t,n))}function LN(n,e){return(n%e+e)%e}function Om(n,e,t){return(1-t)*n+t*e}function Va(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function un(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var fe=class n{constructor(e=0,t=0){n.prototype.isVector2=!0,this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Je(this.x,e.x,t.x),this.y=Je(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=Je(this.x,e,t),this.y=Je(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Je(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Je(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},je=class n{constructor(e,t,i,r,s,o,a,c,l){n.prototype.isMatrix3=!0,this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],d=i[7],f=i[2],h=i[5],g=i[8],v=r[0],m=r[3],p=r[6],T=r[1],M=r[4],E=r[7],I=r[2],D=r[5],C=r[8];return s[0]=o*v+a*T+c*I,s[3]=o*m+a*M+c*D,s[6]=o*p+a*E+c*C,s[1]=l*v+u*T+d*I,s[4]=l*m+u*M+d*D,s[7]=l*p+u*E+d*C,s[2]=f*v+h*T+g*I,s[5]=f*m+h*M+g*D,s[8]=f*p+h*E+g*C,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=u*o-a*l,f=a*c-u*s,h=l*s-o*c,g=t*d+i*f+r*h;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let v=1/g;return e[0]=d*v,e[1]=(r*l-u*i)*v,e[2]=(a*i-r*o)*v,e[3]=f*v,e[4]=(u*t-r*c)*v,e[5]=(r*s-a*t)*v,e[6]=h*v,e[7]=(i*c-l*t)*v,e[8]=(o*t-i*s)*v,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(Lm.makeScale(e,t)),this}rotate(e){return this.premultiply(Lm.makeRotation(-e)),this}translate(e,t){return this.premultiply(Lm.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},Lm=new je;function Fg(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function No(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function cS(){let n=No("canvas");return n.style.display="block",n}var JE={};function gs(n){n in JE||(JE[n]=!0,console.warn(n))}function lS(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}function uS(n){let e=n.elements;e[2]=.5*e[2]+.5*e[3],e[6]=.5*e[6]+.5*e[7],e[10]=.5*e[10]+.5*e[11],e[14]=.5*e[14]+.5*e[15]}function dS(n){let e=n.elements;e[11]===-1?(e[10]=-e[10]-1,e[14]=-e[14]):(e[10]=-e[10],e[14]=-e[14]+1)}var QE=new je().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),eM=new je().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function FN(){let n={enabled:!0,workingColorSpace:as,spaces:{},convert:function(r,s,o){return this.enabled===!1||s===o||!s||!o||(this.spaces[s].transfer===_t&&(r.r=Bi(r.r),r.g=Bi(r.g),r.b=Bi(r.b)),this.spaces[s].primaries!==this.spaces[o].primaries&&(r.applyMatrix3(this.spaces[s].toXYZ),r.applyMatrix3(this.spaces[o].fromXYZ)),this.spaces[o].transfer===_t&&(r.r=Ro(r.r),r.g=Ro(r.g),r.b=Ro(r.b))),r},fromWorkingColorSpace:function(r,s){return this.convert(r,this.workingColorSpace,s)},toWorkingColorSpace:function(r,s){return this.convert(r,s,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===Wi?Xa:this.spaces[r].transfer},getLuminanceCoefficients:function(r,s=this.workingColorSpace){return r.fromArray(this.spaces[s].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,s,o){return r.copy(this.spaces[s].toXYZ).multiply(this.spaces[o].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[as]:{primaries:e,whitePoint:i,transfer:Xa,toXYZ:QE,fromXYZ:eM,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:Tn},outputColorSpaceConfig:{drawingBufferColorSpace:Tn}},[Tn]:{primaries:e,whitePoint:i,transfer:_t,toXYZ:QE,fromXYZ:eM,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:Tn}}}),n}var ct=FN();function Bi(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function Ro(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var yo,id=class{static getDataURL(e){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let t;if(e instanceof HTMLCanvasElement)t=e;else{yo===void 0&&(yo=No("canvas")),yo.width=e.width,yo.height=e.height;let i=yo.getContext("2d");e instanceof ImageData?i.putImageData(e,0,0):i.drawImage(e,0,0,e.width,e.height),t=yo}return t.toDataURL("image/png")}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=No("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=Bi(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(Bi(t[i]/255)*255):t[i]=Bi(t[i]);return{data:t,width:e.width,height:e.height}}else return console.warn("THREE.ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},kN=0,Za=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:kN++}),this.uuid=Ho(),this.data=e,this.dataReady=!0,this.version=0}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(Fm(r[o].image)):s.push(Fm(r[o]))}else s=Fm(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function Fm(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?id.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(console.warn("THREE.Texture: Unable to serialize Texture."),{})}var UN=0,$i=(()=>{class n extends zi{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=gr,s=gr,o=Qn,a=wr,c=Hn,l=mi,u=n.DEFAULT_ANISOTROPY,d=Wi){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:UN++}),this.uuid=Ho(),this.name="",this.source=new Za(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new fe(0,0),this.repeat=new fe(1,1),this.center=new fe(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new je,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.pmremVersion=0}get image(){return this.source.data}set image(t=null){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.6,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==sg)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case Qu:t.x=t.x-Math.floor(t.x);break;case gr:t.x=t.x<0?0:1;break;case ed:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case Qu:t.y=t.y-Math.floor(t.y);break;case gr:t.y=t.y<0?0:1;break;case ed:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=sg,n.DEFAULT_ANISOTROPY=1,n})(),Tt=class n{constructor(e=0,t=0,i=0,r=1){n.prototype.isVector4=!0,this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],d=c[8],f=c[1],h=c[5],g=c[9],v=c[2],m=c[6],p=c[10];if(Math.abs(u-f)<.01&&Math.abs(d-v)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+f)<.1&&Math.abs(d+v)<.1&&Math.abs(g+m)<.1&&Math.abs(l+h+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let M=(l+1)/2,E=(h+1)/2,I=(p+1)/2,D=(u+f)/4,C=(d+v)/4,N=(g+m)/4;return M>E&&M>I?M<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(M),r=D/i,s=C/i):E>I?E<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(E),i=D/r,s=N/r):I<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(I),i=C/s,r=N/s),this.set(i,r,s,t),this}let T=Math.sqrt((m-g)*(m-g)+(d-v)*(d-v)+(f-u)*(f-u));return Math.abs(T)<.001&&(T=1),this.x=(m-g)/T,this.y=(d-v)/T,this.z=(f-u)/T,this.w=Math.acos((l+h+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Je(this.x,e.x,t.x),this.y=Je(this.y,e.y,t.y),this.z=Je(this.z,e.z,t.z),this.w=Je(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=Je(this.x,e,t),this.y=Je(this.y,e,t),this.z=Je(this.z,e,t),this.w=Je(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Je(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},rd=class extends zi{constructor(e=1,t=1,i={}){super(),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=1,this.scissor=new Tt(0,0,e,t),this.scissorTest=!1,this.viewport=new Tt(0,0,e,t);let r={width:e,height:t,depth:1};i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:Qn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1},i);let s=new $i(r,i.mapping,i.wrapS,i.wrapT,i.magFilter,i.minFilter,i.format,i.type,i.anisotropy,i.colorSpace);s.flipY=!1,s.generateMipmaps=i.generateMipmaps,s.internalFormat=i.internalFormat,this.textures=[];let o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&(this._depthTexture.renderTarget=null),e!==null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i;this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let i=0,r=e.textures.length;i<r;i++)this.textures[i]=e.textures[i].clone(),this.textures[i].isRenderTargetTexture=!0,this.textures[i].renderTarget=this;let t=Object.assign({},e.texture.image);return this.texture.source=new Za(t),this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this}dispose(){this.dispatchEvent({type:"dispose"})}},ui=class extends rd{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},Ka=class extends $i{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Vn,this.minFilter=Vn,this.wrapR=gr,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var sd=class extends $i{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Vn,this.minFilter=Vn,this.wrapR=gr,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Hi=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],d=i[r+3],f=s[o+0],h=s[o+1],g=s[o+2],v=s[o+3];if(a===0){e[t+0]=c,e[t+1]=l,e[t+2]=u,e[t+3]=d;return}if(a===1){e[t+0]=f,e[t+1]=h,e[t+2]=g,e[t+3]=v;return}if(d!==v||c!==f||l!==h||u!==g){let m=1-a,p=c*f+l*h+u*g+d*v,T=p>=0?1:-1,M=1-p*p;if(M>Number.EPSILON){let I=Math.sqrt(M),D=Math.atan2(I,p*T);m=Math.sin(m*D)/I,a=Math.sin(a*D)/I}let E=a*T;if(c=c*m+f*E,l=l*m+h*E,u=u*m+g*E,d=d*m+v*E,m===1-a){let I=1/Math.sqrt(c*c+l*l+u*u+d*d);c*=I,l*=I,u*=I,d*=I}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],d=s[o],f=s[o+1],h=s[o+2],g=s[o+3];return e[t]=a*g+u*d+c*h-l*f,e[t+1]=c*g+u*f+l*d-a*h,e[t+2]=l*g+u*h+a*f-c*d,e[t+3]=u*g-a*d-c*f-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),d=a(s/2),f=c(i/2),h=c(r/2),g=c(s/2);switch(o){case"XYZ":this._x=f*u*d+l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d-f*h*g;break;case"YXZ":this._x=f*u*d+l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d+f*h*g;break;case"ZXY":this._x=f*u*d-l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d-f*h*g;break;case"ZYX":this._x=f*u*d-l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d+f*h*g;break;case"YZX":this._x=f*u*d+l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d-f*h*g;break;case"XZY":this._x=f*u*d-l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d+f*h*g;break;default:console.warn("THREE.Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],d=t[10],f=i+a+d;if(f>0){let h=.5/Math.sqrt(f+1);this._w=.25/h,this._x=(u-c)*h,this._y=(s-l)*h,this._z=(o-r)*h}else if(i>a&&i>d){let h=2*Math.sqrt(1+i-a-d);this._w=(u-c)/h,this._x=.25*h,this._y=(r+o)/h,this._z=(s+l)/h}else if(a>d){let h=2*Math.sqrt(1+a-i-d);this._w=(s-l)/h,this._x=(r+o)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+d-i-a);this._w=(o-r)/h,this._x=(s+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<Number.EPSILON?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(Je(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){if(t===0)return this;if(t===1)return this.copy(e);let i=this._x,r=this._y,s=this._z,o=this._w,a=o*e._w+i*e._x+r*e._y+s*e._z;if(a<0?(this._w=-e._w,this._x=-e._x,this._y=-e._y,this._z=-e._z,a=-a):this.copy(e),a>=1)return this._w=o,this._x=i,this._y=r,this._z=s,this;let c=1-a*a;if(c<=Number.EPSILON){let h=1-t;return this._w=h*o+t*this._w,this._x=h*i+t*this._x,this._y=h*r+t*this._y,this._z=h*s+t*this._z,this.normalize(),this}let l=Math.sqrt(c),u=Math.atan2(l,a),d=Math.sin((1-t)*u)/l,f=Math.sin(t*u)/l;return this._w=o*d+this._w*f,this._x=i*d+this._x*f,this._y=r*d+this._y*f,this._z=s*d+this._z*f,this._onChangeCallback(),this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},L=class n{constructor(e=0,t=0,i=0){n.prototype.isVector3=!0,this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(tM.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(tM.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+c*l+o*d-a*u,this.y=i+c*u+a*l-s*d,this.z=r+c*d+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Je(this.x,e.x,t.x),this.y=Je(this.y,e.y,t.y),this.z=Je(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=Je(this.x,e,t),this.y=Je(this.y,e,t),this.z=Je(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Je(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return km.copy(this).projectOnVector(e),this.sub(km)}reflect(e){return this.sub(km.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Je(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},km=new L,tM=new Hi,yr=class{constructor(e=new L(1/0,1/0,1/0),t=new L(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(Zn.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(Zn.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=Zn.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,Zn):Zn.fromBufferAttribute(s,o),Zn.applyMatrix4(e.matrixWorld),this.expandByPoint(Zn);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),Au.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),Au.copy(i.boundingBox)),Au.applyMatrix4(e.matrixWorld),this.union(Au)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,Zn),Zn.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(za),Iu.subVectors(this.max,za),_o.subVectors(e.a,za),xo.subVectors(e.b,za),Eo.subVectors(e.c,za),lr.subVectors(xo,_o),ur.subVectors(Eo,xo),Yr.subVectors(_o,Eo);let t=[0,-lr.z,lr.y,0,-ur.z,ur.y,0,-Yr.z,Yr.y,lr.z,0,-lr.x,ur.z,0,-ur.x,Yr.z,0,-Yr.x,-lr.y,lr.x,0,-ur.y,ur.x,0,-Yr.y,Yr.x,0];return!Um(t,_o,xo,Eo,Iu)||(t=[1,0,0,0,1,0,0,0,1],!Um(t,_o,xo,Eo,Iu))?!1:(Ru.crossVectors(lr,ur),t=[Ru.x,Ru.y,Ru.z],Um(t,_o,xo,Eo,Iu))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,Zn).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(Zn).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(Pi[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),Pi[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),Pi[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),Pi[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),Pi[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),Pi[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),Pi[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),Pi[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(Pi),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}},Pi=[new L,new L,new L,new L,new L,new L,new L,new L],Zn=new L,Au=new yr,_o=new L,xo=new L,Eo=new L,lr=new L,ur=new L,Yr=new L,za=new L,Iu=new L,Ru=new L,Zr=new L;function Um(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){Zr.fromArray(n,s);let a=r.x*Math.abs(Zr.x)+r.y*Math.abs(Zr.y)+r.z*Math.abs(Zr.z),c=e.dot(Zr),l=t.dot(Zr),u=i.dot(Zr);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var BN=new yr,Ha=new L,Bm=new L,Po=class{constructor(e=new L,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):BN.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;Ha.subVectors(e,this.center);let t=Ha.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(Ha,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(Bm.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(Ha.copy(e.center).add(Bm)),this.expandByPoint(Ha.copy(e.center).sub(Bm))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}},Oi=new L,Vm=new L,Nu=new L,dr=new L,zm=new L,Pu=new L,Hm=new L,od=class{constructor(e=new L,t=new L(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,Oi)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=Oi.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(Oi.copy(this.origin).addScaledVector(this.direction,t),Oi.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){Vm.copy(e).add(t).multiplyScalar(.5),Nu.copy(t).sub(e).normalize(),dr.copy(this.origin).sub(Vm);let s=e.distanceTo(t)*.5,o=-this.direction.dot(Nu),a=dr.dot(this.direction),c=-dr.dot(Nu),l=dr.lengthSq(),u=Math.abs(1-o*o),d,f,h,g;if(u>0)if(d=o*c-a,f=o*a-c,g=s*u,d>=0)if(f>=-g)if(f<=g){let v=1/u;d*=v,f*=v,h=d*(d+o*f+2*a)+f*(o*d+f+2*c)+l}else f=s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;else f=-s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;else f<=-g?(d=Math.max(0,-(-o*s+a)),f=d>0?-s:Math.min(Math.max(-s,-c),s),h=-d*d+f*(f+2*c)+l):f<=g?(d=0,f=Math.min(Math.max(-s,-c),s),h=f*(f+2*c)+l):(d=Math.max(0,-(o*s+a)),f=d>0?s:Math.min(Math.max(-s,-c),s),h=-d*d+f*(f+2*c)+l);else f=o>0?-s:s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(Vm).addScaledVector(Nu,f),h}intersectSphere(e,t){Oi.subVectors(e.center,this.origin);let i=Oi.dot(this.direction),r=Oi.dot(Oi)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,f=this.origin;return l>=0?(i=(e.min.x-f.x)*l,r=(e.max.x-f.x)*l):(i=(e.max.x-f.x)*l,r=(e.min.x-f.x)*l),u>=0?(s=(e.min.y-f.y)*u,o=(e.max.y-f.y)*u):(s=(e.max.y-f.y)*u,o=(e.min.y-f.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-f.z)*d,c=(e.max.z-f.z)*d):(a=(e.max.z-f.z)*d,c=(e.min.z-f.z)*d),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,Oi)!==null}intersectTriangle(e,t,i,r,s){zm.subVectors(t,e),Pu.subVectors(i,e),Hm.crossVectors(zm,Pu);let o=this.direction.dot(Hm),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;dr.subVectors(this.origin,e);let c=a*this.direction.dot(Pu.crossVectors(dr,Pu));if(c<0)return null;let l=a*this.direction.dot(zm.cross(dr));if(l<0||c+l>o)return null;let u=-a*dr.dot(Hm);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},bt=class n{constructor(e,t,i,r,s,o,a,c,l,u,d,f,h,g,v,m){n.prototype.isMatrix4=!0,this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,d,f,h,g,v,m)}set(e,t,i,r,s,o,a,c,l,u,d,f,h,g,v,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=d,p[14]=f,p[3]=h,p[7]=g,p[11]=v,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){let t=this.elements,i=e.elements,r=1/Mo.setFromMatrixColumn(e,0).length(),s=1/Mo.setFromMatrixColumn(e,1).length(),o=1/Mo.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let f=o*u,h=o*d,g=a*u,v=a*d;t[0]=c*u,t[4]=-c*d,t[8]=l,t[1]=h+g*l,t[5]=f-v*l,t[9]=-a*c,t[2]=v-f*l,t[6]=g+h*l,t[10]=o*c}else if(e.order==="YXZ"){let f=c*u,h=c*d,g=l*u,v=l*d;t[0]=f+v*a,t[4]=g*a-h,t[8]=o*l,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=h*a-g,t[6]=v+f*a,t[10]=o*c}else if(e.order==="ZXY"){let f=c*u,h=c*d,g=l*u,v=l*d;t[0]=f-v*a,t[4]=-o*d,t[8]=g+h*a,t[1]=h+g*a,t[5]=o*u,t[9]=v-f*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let f=o*u,h=o*d,g=a*u,v=a*d;t[0]=c*u,t[4]=g*l-h,t[8]=f*l+v,t[1]=c*d,t[5]=v*l+f,t[9]=h*l-g,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let f=o*c,h=o*l,g=a*c,v=a*l;t[0]=c*u,t[4]=v-f*d,t[8]=g*d+h,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*d+g,t[10]=f-v*d}else if(e.order==="XZY"){let f=o*c,h=o*l,g=a*c,v=a*l;t[0]=c*u,t[4]=-d,t[8]=l*u,t[1]=f*d+v,t[5]=o*u,t[9]=h*d-g,t[2]=g*d-h,t[6]=a*u,t[10]=v*d+f}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(VN,e,zN)}lookAt(e,t,i){let r=this.elements;return wn.subVectors(e,t),wn.lengthSq()===0&&(wn.z=1),wn.normalize(),fr.crossVectors(i,wn),fr.lengthSq()===0&&(Math.abs(i.z)===1?wn.x+=1e-4:wn.z+=1e-4,wn.normalize(),fr.crossVectors(i,wn)),fr.normalize(),Ou.crossVectors(wn,fr),r[0]=fr.x,r[4]=Ou.x,r[8]=wn.x,r[1]=fr.y,r[5]=Ou.y,r[9]=wn.y,r[2]=fr.z,r[6]=Ou.z,r[10]=wn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],d=i[5],f=i[9],h=i[13],g=i[2],v=i[6],m=i[10],p=i[14],T=i[3],M=i[7],E=i[11],I=i[15],D=r[0],C=r[4],N=r[8],w=r[12],x=r[1],A=r[5],$=r[9],B=r[13],q=r[2],X=r[6],H=r[10],Z=r[14],V=r[3],ae=r[7],ge=r[11],Se=r[15];return s[0]=o*D+a*x+c*q+l*V,s[4]=o*C+a*A+c*X+l*ae,s[8]=o*N+a*$+c*H+l*ge,s[12]=o*w+a*B+c*Z+l*Se,s[1]=u*D+d*x+f*q+h*V,s[5]=u*C+d*A+f*X+h*ae,s[9]=u*N+d*$+f*H+h*ge,s[13]=u*w+d*B+f*Z+h*Se,s[2]=g*D+v*x+m*q+p*V,s[6]=g*C+v*A+m*X+p*ae,s[10]=g*N+v*$+m*H+p*ge,s[14]=g*w+v*B+m*Z+p*Se,s[3]=T*D+M*x+E*q+I*V,s[7]=T*C+M*A+E*X+I*ae,s[11]=T*N+M*$+E*H+I*ge,s[15]=T*w+M*B+E*Z+I*Se,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],d=e[6],f=e[10],h=e[14],g=e[3],v=e[7],m=e[11],p=e[15];return g*(+s*c*d-r*l*d-s*a*f+i*l*f+r*a*h-i*c*h)+v*(+t*c*h-t*l*f+s*o*f-r*o*h+r*l*u-s*c*u)+m*(+t*l*d-t*a*h-s*o*d+i*o*h+s*a*u-i*l*u)+p*(-r*a*u-t*c*d+t*a*f+r*o*d-i*o*f+i*c*u)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=e[9],f=e[10],h=e[11],g=e[12],v=e[13],m=e[14],p=e[15],T=d*m*l-v*f*l+v*c*h-a*m*h-d*c*p+a*f*p,M=g*f*l-u*m*l-g*c*h+o*m*h+u*c*p-o*f*p,E=u*v*l-g*d*l+g*a*h-o*v*h-u*a*p+o*d*p,I=g*d*c-u*v*c-g*a*f+o*v*f+u*a*m-o*d*m,D=t*T+i*M+r*E+s*I;if(D===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let C=1/D;return e[0]=T*C,e[1]=(v*f*s-d*m*s-v*r*h+i*m*h+d*r*p-i*f*p)*C,e[2]=(a*m*s-v*c*s+v*r*l-i*m*l-a*r*p+i*c*p)*C,e[3]=(d*c*s-a*f*s-d*r*l+i*f*l+a*r*h-i*c*h)*C,e[4]=M*C,e[5]=(u*m*s-g*f*s+g*r*h-t*m*h-u*r*p+t*f*p)*C,e[6]=(g*c*s-o*m*s-g*r*l+t*m*l+o*r*p-t*c*p)*C,e[7]=(o*f*s-u*c*s+u*r*l-t*f*l-o*r*h+t*c*h)*C,e[8]=E*C,e[9]=(g*d*s-u*v*s-g*i*h+t*v*h+u*i*p-t*d*p)*C,e[10]=(o*v*s-g*a*s+g*i*l-t*v*l-o*i*p+t*a*p)*C,e[11]=(u*a*s-o*d*s-u*i*l+t*d*l+o*i*h-t*a*h)*C,e[12]=I*C,e[13]=(u*v*r-g*d*r+g*i*f-t*v*f-u*i*m+t*d*m)*C,e[14]=(g*a*r-o*v*r-g*i*c+t*v*c+o*i*m-t*a*m)*C,e[15]=(o*d*r-u*a*r+u*i*c-t*d*c-o*i*f+t*a*f)*C,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,d=a+a,f=s*l,h=s*u,g=s*d,v=o*u,m=o*d,p=a*d,T=c*l,M=c*u,E=c*d,I=i.x,D=i.y,C=i.z;return r[0]=(1-(v+p))*I,r[1]=(h+E)*I,r[2]=(g-M)*I,r[3]=0,r[4]=(h-E)*D,r[5]=(1-(f+p))*D,r[6]=(m+T)*D,r[7]=0,r[8]=(g+M)*C,r[9]=(m-T)*C,r[10]=(1-(f+v))*C,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements,s=Mo.set(r[0],r[1],r[2]).length(),o=Mo.set(r[4],r[5],r[6]).length(),a=Mo.set(r[8],r[9],r[10]).length();this.determinant()<0&&(s=-s),e.x=r[12],e.y=r[13],e.z=r[14],Kn.copy(this);let l=1/s,u=1/o,d=1/a;return Kn.elements[0]*=l,Kn.elements[1]*=l,Kn.elements[2]*=l,Kn.elements[4]*=u,Kn.elements[5]*=u,Kn.elements[6]*=u,Kn.elements[8]*=d,Kn.elements[9]*=d,Kn.elements[10]*=d,t.setFromRotationMatrix(Kn),i.x=s,i.y=o,i.z=a,this}makePerspective(e,t,i,r,s,o,a=li){let c=this.elements,l=2*s/(t-e),u=2*s/(i-r),d=(t+e)/(t-e),f=(i+r)/(i-r),h,g;if(a===li)h=-(o+s)/(o-s),g=-2*o*s/(o-s);else if(a===Ya)h=-o/(o-s),g=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return c[0]=l,c[4]=0,c[8]=d,c[12]=0,c[1]=0,c[5]=u,c[9]=f,c[13]=0,c[2]=0,c[6]=0,c[10]=h,c[14]=g,c[3]=0,c[7]=0,c[11]=-1,c[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=li){let c=this.elements,l=1/(t-e),u=1/(i-r),d=1/(o-s),f=(t+e)*l,h=(i+r)*u,g,v;if(a===li)g=(o+s)*d,v=-2*d;else if(a===Ya)g=s*d,v=-1*d;else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return c[0]=2*l,c[4]=0,c[8]=0,c[12]=-f,c[1]=0,c[5]=2*u,c[9]=0,c[13]=-h,c[2]=0,c[6]=0,c[10]=v,c[14]=-g,c[3]=0,c[7]=0,c[11]=0,c[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},Mo=new L,Kn=new bt,VN=new L(0,0,0),zN=new L(1,1,1),fr=new L,Ou=new L,wn=new L,nM=new bt,iM=new Hi,_r=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],c=s[8],l=s[1],u=s[5],d=s[9],f=s[2],h=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(Je(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-Je(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(c,g),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-f,o),this._z=0);break;case"ZXY":this._x=Math.asin(Je(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-f,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,o));break;case"ZYX":this._y=Math.asin(-Je(f,-1,1)),Math.abs(f)<.9999999?(this._x=Math.atan2(h,g),this._z=Math.atan2(l,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(Je(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-f,o)):(this._x=0,this._y=Math.atan2(c,g));break;case"XZY":this._z=Math.asin(-Je(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(c,o)):(this._x=Math.atan2(-d,g),this._y=0);break;default:console.warn("THREE.Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return nM.makeRotationFromQuaternion(t),this.setFromRotationMatrix(nM,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return iM.setFromEuler(this),this.setFromQuaternion(iM,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),Ja=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},HN=0,rM=new L,So=new Hi,Li=new bt,Lu=new L,Ga=new L,GN=new L,jN=new Hi,sM=new L(1,0,0),oM=new L(0,1,0),aM=new L(0,0,1),cM={type:"added"},WN={type:"removed"},wo={type:"childadded",child:null},Gm={type:"childremoved",child:null},di=(()=>{class n extends zi{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:HN++}),this.uuid=Ho(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new L,i=new _r,r=new Hi,s=new L(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new bt},normalMatrix:{value:new je}}),this.matrix=new bt,this.matrixWorld=new bt,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new Ja,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.userData={}}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return So.setFromAxisAngle(t,i),this.quaternion.multiply(So),this}rotateOnWorldAxis(t,i){return So.setFromAxisAngle(t,i),this.quaternion.premultiply(So),this}rotateX(t){return this.rotateOnAxis(sM,t)}rotateY(t){return this.rotateOnAxis(oM,t)}rotateZ(t){return this.rotateOnAxis(aM,t)}translateOnAxis(t,i){return rM.copy(t).applyQuaternion(this.quaternion),this.position.add(rM.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(sM,t)}translateY(t){return this.translateOnAxis(oM,t)}translateZ(t){return this.translateOnAxis(aM,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(Li.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?Lu.copy(t):Lu.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),Ga.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?Li.lookAt(Ga,Lu,this.up):Li.lookAt(Lu,Ga,this.up),this.quaternion.setFromRotationMatrix(Li),s&&(Li.extractRotation(s.matrixWorld),So.setFromRotationMatrix(Li),this.quaternion.premultiply(So.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(console.error("THREE.Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(cM),wo.child=t,this.dispatchEvent(wo),wo.child=null):console.error("THREE.Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(WN),Gm.child=t,this.dispatchEvent(Gm),Gm.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),Li.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),Li.multiply(t.parent.matrixWorld)),t.applyMatrix4(Li),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(cM),wo.child=t,this.dispatchEvent(wo),wo.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Ga,t,GN),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Ga,jN,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale),this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.6,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.visibility=this._visibility,s.active=this._active,s.bounds=this._bounds.map(c=>({boxInitialized:c.boxInitialized,boxMin:c.box.min.toArray(),boxMax:c.box.max.toArray(),sphereInitialized:c.sphereInitialized,sphereRadius:c.sphere.radius,sphereCenter:c.sphere.center.toArray()})),s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.geometryCount=this._geometryCount,s.matricesTexture=this._matricesTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere={center:s.boundingSphere.center.toArray(),radius:s.boundingSphere.radius}),this.boundingBox!==null&&(s.boundingBox={min:s.boundingBox.min.toArray(),max:s.boundingBox.max.toArray()}));function o(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,d=l.length;u<d;u++){let f=l[u];o(t.shapes,f)}else o(t.shapes,l)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(o(t.materials,this.material[l]));s.material=c}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let c=0;c<this.children.length;c++)s.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];s.animations.push(o(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),d=a(t.images),f=a(t.shapes),h=a(t.skeletons),g=a(t.animations),v=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),f.length>0&&(r.shapes=f),h.length>0&&(r.skeletons=h),g.length>0&&(r.animations=g),v.length>0&&(r.nodes=v)}return r.object=s,r;function a(c){let l=[];for(let u in c){let d=c[u];delete d.metadata,l.push(d)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new L(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),Jn=new L,Fi=new L,jm=new L,ki=new L,bo=new L,To=new L,lM=new L,Wm=new L,$m=new L,qm=new L,Xm=new Tt,Ym=new Tt,Zm=new Tt,mr=class n{constructor(e=new L,t=new L,i=new L){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Jn.subVectors(e,t),r.cross(Jn);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){Jn.subVectors(r,t),Fi.subVectors(i,t),jm.subVectors(e,t);let o=Jn.dot(Jn),a=Jn.dot(Fi),c=Jn.dot(jm),l=Fi.dot(Fi),u=Fi.dot(jm),d=o*l-a*a;if(d===0)return s.set(0,0,0),null;let f=1/d,h=(l*c-a*u)*f,g=(o*u-a*c)*f;return s.set(1-h-g,g,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,ki)===null?!1:ki.x>=0&&ki.y>=0&&ki.x+ki.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,ki)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,ki.x),c.addScaledVector(o,ki.y),c.addScaledVector(a,ki.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return Xm.setScalar(0),Ym.setScalar(0),Zm.setScalar(0),Xm.fromBufferAttribute(e,t),Ym.fromBufferAttribute(e,i),Zm.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(Xm,s.x),o.addScaledVector(Ym,s.y),o.addScaledVector(Zm,s.z),o}static isFrontFacing(e,t,i,r){return Jn.subVectors(i,t),Fi.subVectors(e,t),Jn.cross(Fi).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Jn.subVectors(this.c,this.b),Fi.subVectors(this.a,this.b),Jn.cross(Fi).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;bo.subVectors(r,i),To.subVectors(s,i),Wm.subVectors(e,i);let c=bo.dot(Wm),l=To.dot(Wm);if(c<=0&&l<=0)return t.copy(i);$m.subVectors(e,r);let u=bo.dot($m),d=To.dot($m);if(u>=0&&d<=u)return t.copy(r);let f=c*d-u*l;if(f<=0&&c>=0&&u<=0)return o=c/(c-u),t.copy(i).addScaledVector(bo,o);qm.subVectors(e,s);let h=bo.dot(qm),g=To.dot(qm);if(g>=0&&h<=g)return t.copy(s);let v=h*l-c*g;if(v<=0&&l>=0&&g<=0)return a=l/(l-g),t.copy(i).addScaledVector(To,a);let m=u*g-h*d;if(m<=0&&d-u>=0&&h-g>=0)return lM.subVectors(s,r),a=(d-u)/(d-u+(h-g)),t.copy(r).addScaledVector(lM,a);let p=1/(m+v+f);return o=v*p,a=f*p,t.copy(i).addScaledVector(bo,o).addScaledVector(To,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},fS={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},hr={h:0,s:0,l:0},Fu={h:0,s:0,l:0};function Km(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var tt=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Tn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,ct.toWorkingColorSpace(this,t),this}setRGB(e,t,i,r=ct.workingColorSpace){return this.r=e,this.g=t,this.b=i,ct.toWorkingColorSpace(this,r),this}setHSL(e,t,i,r=ct.workingColorSpace){if(e=LN(e,1),t=Je(t,0,1),i=Je(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=Km(o,s,e+1/3),this.g=Km(o,s,e),this.b=Km(o,s,e-1/3)}return ct.toWorkingColorSpace(this,r),this}setStyle(e,t=Tn){function i(s){s!==void 0&&parseFloat(s)<1&&console.warn("THREE.Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:console.warn("THREE.Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);console.warn("THREE.Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Tn){let i=fS[e.toLowerCase()];return i!==void 0?this.setHex(i,t):console.warn("THREE.Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=Bi(e.r),this.g=Bi(e.g),this.b=Bi(e.b),this}copyLinearToSRGB(e){return this.r=Ro(e.r),this.g=Ro(e.g),this.b=Ro(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Tn){return ct.fromWorkingColorSpace(Jt.copy(this),e),Math.round(Je(Jt.r*255,0,255))*65536+Math.round(Je(Jt.g*255,0,255))*256+Math.round(Je(Jt.b*255,0,255))}getHexString(e=Tn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=ct.workingColorSpace){ct.fromWorkingColorSpace(Jt.copy(this),t);let i=Jt.r,r=Jt.g,s=Jt.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let d=o-a;switch(l=u<=.5?d/(o+a):d/(2-o-a),o){case i:c=(r-s)/d+(r<s?6:0);break;case r:c=(s-i)/d+2;break;case s:c=(i-r)/d+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=ct.workingColorSpace){return ct.fromWorkingColorSpace(Jt.copy(this),t),e.r=Jt.r,e.g=Jt.g,e.b=Jt.b,e}getStyle(e=Tn){ct.fromWorkingColorSpace(Jt.copy(this),e);let t=Jt.r,i=Jt.g,r=Jt.b;return e!==Tn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(hr),this.setHSL(hr.h+e,hr.s+t,hr.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(hr),e.getHSL(Fu);let i=Om(hr.h,Fu.h,t),r=Om(hr.s,Fu.s,t),s=Om(hr.l,Fu.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},Jt=new tt;tt.NAMES=fS;var $N=0,xr=class extends zi{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:$N++}),this.uuid=Ho(),this.name="",this.type="Material",this.blending=rs,this.side=Vi,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=Ku,this.blendDst=Ju,this.blendEquation=vr,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new tt(0,0,0),this.blendAlpha=0,this.depthFunc=ss,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=lg,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=Qr,this.stencilZFail=Qr,this.stencilZPass=Qr,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){console.warn(`THREE.Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){console.warn(`THREE.Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.6,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==rs&&(i.blending=this.blending),this.side!==Vi&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==Ku&&(i.blendSrc=this.blendSrc),this.blendDst!==Ju&&(i.blendDst=this.blendDst),this.blendEquation!==vr&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==ss&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==lg&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==Qr&&(i.stencilFail=this.stencilFail),this.stencilZFail!==Qr&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==Qr&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}onBuild(){console.warn("Material: onBuild() has been removed.")}},Er=class extends xr{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new tt(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new _r,this.combine=Ld,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}};var Pt=new L,ku=new fe,qN=0,Cn=class{constructor(e,t,i=!1){if(Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:qN++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=ug,this.updateRanges=[],this.gpuType=gi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)ku.fromBufferAttribute(this,t),ku.applyMatrix3(e),this.setXY(t,ku.x,ku.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Pt.fromBufferAttribute(this,t),Pt.applyMatrix3(e),this.setXYZ(t,Pt.x,Pt.y,Pt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Pt.fromBufferAttribute(this,t),Pt.applyMatrix4(e),this.setXYZ(t,Pt.x,Pt.y,Pt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Pt.fromBufferAttribute(this,t),Pt.applyNormalMatrix(e),this.setXYZ(t,Pt.x,Pt.y,Pt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Pt.fromBufferAttribute(this,t),Pt.transformDirection(e),this.setXYZ(t,Pt.x,Pt.y,Pt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=Va(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=un(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=Va(t,this.array)),t}setX(e,t){return this.normalized&&(t=un(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=Va(t,this.array)),t}setY(e,t){return this.normalized&&(t=un(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=Va(t,this.array)),t}setZ(e,t){return this.normalized&&(t=un(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=Va(t,this.array)),t}setW(e,t){return this.normalized&&(t=un(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=un(t,this.array),i=un(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=un(t,this.array),i=un(i,this.array),r=un(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=un(t,this.array),i=un(i,this.array),r=un(r,this.array),s=un(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==ug&&(e.usage=this.usage),e}};var Qa=class extends Cn{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var ec=class extends Cn{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var rn=class extends Cn{constructor(e,t,i){super(new Float32Array(e),t,i)}},XN=0,Bn=new bt,Jm=new di,Co=new L,bn=new yr,ja=new yr,Ht=new L,fi=class n extends zi{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:XN++}),this.uuid=Ho(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(Fg(e)?ec:Qa)(e,1):this.index=e,this}setIndirect(e){return this.indirect=e,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new je().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return Bn.makeRotationFromQuaternion(e),this.applyMatrix4(Bn),this}rotateX(e){return Bn.makeRotationX(e),this.applyMatrix4(Bn),this}rotateY(e){return Bn.makeRotationY(e),this.applyMatrix4(Bn),this}rotateZ(e){return Bn.makeRotationZ(e),this.applyMatrix4(Bn),this}translate(e,t,i){return Bn.makeTranslation(e,t,i),this.applyMatrix4(Bn),this}scale(e,t,i){return Bn.makeScale(e,t,i),this.applyMatrix4(Bn),this}lookAt(e){return Jm.lookAt(e),Jm.updateMatrix(),this.applyMatrix4(Jm.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(Co).negate(),this.translate(Co.x,Co.y,Co.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new rn(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let s=e[r];t.setXYZ(r,s.x,s.y,s.z||0)}e.length>t.count&&console.warn("THREE.BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new yr);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new L(-1/0,-1/0,-1/0),new L(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];bn.setFromBufferAttribute(s),this.morphTargetsRelative?(Ht.addVectors(this.boundingBox.min,bn.min),this.boundingBox.expandByPoint(Ht),Ht.addVectors(this.boundingBox.max,bn.max),this.boundingBox.expandByPoint(Ht)):(this.boundingBox.expandByPoint(bn.min),this.boundingBox.expandByPoint(bn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&console.error('THREE.BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Po);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new L,1/0);return}if(e){let i=this.boundingSphere.center;if(bn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];ja.setFromBufferAttribute(a),this.morphTargetsRelative?(Ht.addVectors(bn.min,ja.min),bn.expandByPoint(Ht),Ht.addVectors(bn.max,ja.max),bn.expandByPoint(Ht)):(bn.expandByPoint(ja.min),bn.expandByPoint(ja.max))}bn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)Ht.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(Ht));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)Ht.fromBufferAttribute(a,l),c&&(Co.fromBufferAttribute(e,l),Ht.add(Co)),r=Math.max(r,i.distanceToSquared(Ht))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&console.error('THREE.BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){console.error("THREE.BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new Cn(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],c=[];for(let N=0;N<i.count;N++)a[N]=new L,c[N]=new L;let l=new L,u=new L,d=new L,f=new fe,h=new fe,g=new fe,v=new L,m=new L;function p(N,w,x){l.fromBufferAttribute(i,N),u.fromBufferAttribute(i,w),d.fromBufferAttribute(i,x),f.fromBufferAttribute(s,N),h.fromBufferAttribute(s,w),g.fromBufferAttribute(s,x),u.sub(l),d.sub(l),h.sub(f),g.sub(f);let A=1/(h.x*g.y-g.x*h.y);isFinite(A)&&(v.copy(u).multiplyScalar(g.y).addScaledVector(d,-h.y).multiplyScalar(A),m.copy(d).multiplyScalar(h.x).addScaledVector(u,-g.x).multiplyScalar(A),a[N].add(v),a[w].add(v),a[x].add(v),c[N].add(m),c[w].add(m),c[x].add(m))}let T=this.groups;T.length===0&&(T=[{start:0,count:e.count}]);for(let N=0,w=T.length;N<w;++N){let x=T[N],A=x.start,$=x.count;for(let B=A,q=A+$;B<q;B+=3)p(e.getX(B+0),e.getX(B+1),e.getX(B+2))}let M=new L,E=new L,I=new L,D=new L;function C(N){I.fromBufferAttribute(r,N),D.copy(I);let w=a[N];M.copy(w),M.sub(I.multiplyScalar(I.dot(w))).normalize(),E.crossVectors(D,w);let A=E.dot(c[N])<0?-1:1;o.setXYZW(N,M.x,M.y,M.z,A)}for(let N=0,w=T.length;N<w;++N){let x=T[N],A=x.start,$=x.count;for(let B=A,q=A+$;B<q;B+=3)C(e.getX(B+0)),C(e.getX(B+1)),C(e.getX(B+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new Cn(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let f=0,h=i.count;f<h;f++)i.setXYZ(f,0,0,0);let r=new L,s=new L,o=new L,a=new L,c=new L,l=new L,u=new L,d=new L;if(e)for(let f=0,h=e.count;f<h;f+=3){let g=e.getX(f+0),v=e.getX(f+1),m=e.getX(f+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,v),o.fromBufferAttribute(t,m),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,g),c.fromBufferAttribute(i,v),l.fromBufferAttribute(i,m),a.add(u),c.add(u),l.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(v,c.x,c.y,c.z),i.setXYZ(m,l.x,l.y,l.z)}else for(let f=0,h=t.count;f<h;f+=3)r.fromBufferAttribute(t,f+0),s.fromBufferAttribute(t,f+1),o.fromBufferAttribute(t,f+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(f+0,u.x,u.y,u.z),i.setXYZ(f+1,u.x,u.y,u.z),i.setXYZ(f+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)Ht.fromBufferAttribute(e,t),Ht.normalize(),e.setXYZ(t,Ht.x,Ht.y,Ht.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,d=a.normalized,f=new l.constructor(c.length*u),h=0,g=0;for(let v=0,m=c.length;v<m;v++){a.isInterleavedBufferAttribute?h=c[v]*a.data.stride+a.offset:h=c[v]*u;for(let p=0;p<u;p++)f[g++]=l[h++]}return new Cn(f,u,d)}if(this.index===null)return console.warn("THREE.BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,d=l.length;u<d;u++){let f=l[u],h=e(f,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.6,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let d=0,f=l.length;d<f;d++){let h=l[d];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere={center:a.center.toArray(),radius:a.radius}),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone(t));let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],d=s[l];for(let f=0,h=d.length;f<h;f++)u.push(d[f].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let d=o[l];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}},uM=new bt,Kr=new od,Uu=new Po,dM=new L,Bu=new L,Vu=new L,zu=new L,Qm=new L,Hu=new L,fM=new L,Gu=new L,Gt=class extends di{constructor(e=new fi,t=new Er){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){Hu.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],d=s[c];u!==0&&(Qm.fromBufferAttribute(d,e),o?Hu.addScaledVector(Qm,u):Hu.addScaledVector(Qm.sub(t),u))}t.add(Hu)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),Uu.copy(i.boundingSphere),Uu.applyMatrix4(s),Kr.copy(e.ray).recast(e.near),!(Uu.containsPoint(Kr.origin)===!1&&(Kr.intersectSphere(Uu,dM)===null||Kr.origin.distanceToSquared(dM)>(e.far-e.near)**2))&&(uM.copy(s).invert(),Kr.copy(e.ray).applyMatrix4(uM),!(i.boundingBox!==null&&Kr.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,Kr)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,f=s.groups,h=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,v=f.length;g<v;g++){let m=f[g],p=o[m.materialIndex],T=Math.max(m.start,h.start),M=Math.min(a.count,Math.min(m.start+m.count,h.start+h.count));for(let E=T,I=M;E<I;E+=3){let D=a.getX(E),C=a.getX(E+1),N=a.getX(E+2);r=ju(this,p,e,i,l,u,d,D,C,N),r&&(r.faceIndex=Math.floor(E/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),v=Math.min(a.count,h.start+h.count);for(let m=g,p=v;m<p;m+=3){let T=a.getX(m),M=a.getX(m+1),E=a.getX(m+2);r=ju(this,o,e,i,l,u,d,T,M,E),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let g=0,v=f.length;g<v;g++){let m=f[g],p=o[m.materialIndex],T=Math.max(m.start,h.start),M=Math.min(c.count,Math.min(m.start+m.count,h.start+h.count));for(let E=T,I=M;E<I;E+=3){let D=E,C=E+1,N=E+2;r=ju(this,p,e,i,l,u,d,D,C,N),r&&(r.faceIndex=Math.floor(E/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),v=Math.min(c.count,h.start+h.count);for(let m=g,p=v;m<p;m+=3){let T=m,M=m+1,E=m+2;r=ju(this,o,e,i,l,u,d,T,M,E),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function YN(n,e,t,i,r,s,o,a){let c;if(e.side===sn?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===Vi,a),c===null)return null;Gu.copy(a),Gu.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(Gu);return l<t.near||l>t.far?null:{distance:l,point:Gu.clone(),object:n}}function ju(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,Bu),n.getVertexPosition(c,Vu),n.getVertexPosition(l,zu);let u=YN(n,e,t,i,Bu,Vu,zu,fM);if(u){let d=new L;mr.getBarycoord(fM,Bu,Vu,zu,d),r&&(u.uv=mr.getInterpolatedAttribute(r,a,c,l,d,new fe)),s&&(u.uv1=mr.getInterpolatedAttribute(s,a,c,l,d,new fe)),o&&(u.normal=mr.getInterpolatedAttribute(o,a,c,l,d,new L),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let f={a,b:c,c:l,normal:new L,materialIndex:0};mr.getNormal(Bu,Vu,zu,f.normal),u.face=f,u.barycoord=d}return u}var Oo=class n extends fi{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],d=[],f=0,h=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new rn(l,3)),this.setAttribute("normal",new rn(u,3)),this.setAttribute("uv",new rn(d,2));function g(v,m,p,T,M,E,I,D,C,N,w){let x=E/C,A=I/N,$=E/2,B=I/2,q=D/2,X=C+1,H=N+1,Z=0,V=0,ae=new L;for(let ge=0;ge<H;ge++){let Se=ge*A-B;for(let Ge=0;Ge<X;Ge++){let dt=Ge*x-$;ae[v]=dt*T,ae[m]=Se*M,ae[p]=q,l.push(ae.x,ae.y,ae.z),ae[v]=0,ae[m]=0,ae[p]=D>0?1:-1,u.push(ae.x,ae.y,ae.z),d.push(Ge/C),d.push(1-ge/N),Z+=1}}for(let ge=0;ge<N;ge++)for(let Se=0;Se<C;Se++){let Ge=f+Se+X*ge,dt=f+Se+X*(ge+1),W=f+(Se+1)+X*(ge+1),se=f+(Se+1)+X*ge;c.push(Ge,dt,se),c.push(dt,W,se),V+=6}a.addGroup(h,V,w),h+=V,f+=Z}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};function vs(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];r&&(r.isColor||r.isMatrix3||r.isMatrix4||r.isVector2||r.isVector3||r.isVector4||r.isTexture||r.isQuaternion)?r.isRenderTargetTexture?(console.warn("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone():Array.isArray(r)?e[t][i]=r.slice():e[t][i]=r}}return e}function en(n){let e={};for(let t=0;t<n.length;t++){let i=vs(n[t]);for(let r in i)e[r]=i[r]}return e}function ZN(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function kg(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:ct.workingColorSpace}var hS={clone:vs,merge:en},KN=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,JN=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,ei=class extends xr{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=KN,this.fragmentShader=JN,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=vs(e.uniforms),this.uniformsGroups=ZN(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},tc=class extends di{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new bt,this.projectionMatrix=new bt,this.projectionMatrixInverse=new bt,this.coordinateSystem=li}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorldInverse.copy(this.matrixWorld).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorldInverse.copy(this.matrixWorld).invert()}clone(){return new this.constructor().copy(this)}},pr=new L,hM=new fe,pM=new fe,Qt=class extends tc{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=nd*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(Pm*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return nd*2*Math.atan(Math.tan(Pm*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){pr.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(pr.x,pr.y).multiplyScalar(-e/pr.z),pr.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(pr.x,pr.y).multiplyScalar(-e/pr.z)}getViewSize(e,t){return this.getViewBounds(e,hM,pM),t.subVectors(pM,hM)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(Pm*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}},Do=-90,Ao=1,ad=class extends di{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new Qt(Do,Ao,e,t);r.layers=this.layers,this.add(r);let s=new Qt(Do,Ao,e,t);s.layers=this.layers,this.add(s);let o=new Qt(Do,Ao,e,t);o.layers=this.layers,this.add(o);let a=new Qt(Do,Ao,e,t);a.layers=this.layers,this.add(a);let c=new Qt(Do,Ao,e,t);c.layers=this.layers,this.add(c);let l=new Qt(Do,Ao,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===li)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===Ya)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,d=e.getRenderTarget(),f=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let v=i.texture.generateMipmaps;i.texture.generateMipmaps=!1,e.setRenderTarget(i,0,r),e.render(t,s),e.setRenderTarget(i,1,r),e.render(t,o),e.setRenderTarget(i,2,r),e.render(t,a),e.setRenderTarget(i,3,r),e.render(t,c),e.setRenderTarget(i,4,r),e.render(t,l),i.texture.generateMipmaps=v,e.setRenderTarget(i,5,r),e.render(t,u),e.setRenderTarget(d,f,h),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},nc=class extends $i{constructor(e,t,i,r,s,o,a,c,l,u){e=e!==void 0?e:[],t=t!==void 0?t:hs,super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}},cd=class extends ui{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new nc(r,t.mapping,t.wrapS,t.wrapT,t.magFilter,t.minFilter,t.format,t.type,t.anisotropy,t.colorSpace),this.texture.isRenderTargetTexture=!0,this.texture.generateMipmaps=t.generateMipmaps!==void 0?t.generateMipmaps:!1,this.texture.minFilter=t.minFilter!==void 0?t.minFilter:Qn}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new Oo(5,5,5),s=new ei({name:"CubemapFromEquirect",uniforms:vs(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:sn,blending:Gi});s.uniforms.tEquirect.value=t;let o=new Gt(r,s),a=t.minFilter;return t.minFilter===wr&&(t.minFilter=Qn),new ad(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t,i,r){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}},es=class extends di{constructor(){super(),this.isGroup=!0,this.type="Group"}},QN={type:"move"},Lo=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new es,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new es,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new L,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new L),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new es,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new L,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new L),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let v of e.hand.values()){let m=t.getJointPose(v,i),p=this._getHandJoint(l,v);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=l.joints["index-finger-tip"],d=l.joints["thumb-tip"],f=u.position.distanceTo(d.position),h=.02,g=.005;l.inputState.pinching&&f>h+g?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&f<=h-g&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(QN)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new es;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}};var ic=class extends di{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new _r,this.environmentIntensity=1,this.environmentRotation=new _r,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}};var eg=new L,eP=new L,tP=new je,ci=class{constructor(e=new L(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=eg.subVectors(i,t).cross(eP.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t){let i=e.delta(eg),r=this.normal.dot(i);if(r===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let s=-(e.start.dot(this.normal)+this.constant)/r;return s<0||s>1?null:t.copy(e.start).addScaledVector(i,s)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||tP.getNormalMatrix(e),r=this.coplanarPoint(eg).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},Jr=new Po,Wu=new L,Fo=class{constructor(e=new ci,t=new ci,i=new ci,r=new ci,s=new ci,o=new ci){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=li){let i=this.planes,r=e.elements,s=r[0],o=r[1],a=r[2],c=r[3],l=r[4],u=r[5],d=r[6],f=r[7],h=r[8],g=r[9],v=r[10],m=r[11],p=r[12],T=r[13],M=r[14],E=r[15];if(i[0].setComponents(c-s,f-l,m-h,E-p).normalize(),i[1].setComponents(c+s,f+l,m+h,E+p).normalize(),i[2].setComponents(c+o,f+u,m+g,E+T).normalize(),i[3].setComponents(c-o,f-u,m-g,E-T).normalize(),i[4].setComponents(c-a,f-d,m-v,E-M).normalize(),t===li)i[5].setComponents(c+a,f+d,m+v,E+M).normalize();else if(t===Ya)i[5].setComponents(a,d,v,M).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),Jr.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),Jr.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(Jr)}intersectsSprite(e){return Jr.center.set(0,0,0),Jr.radius=.7071067811865476,Jr.applyMatrix4(e.matrixWorld),this.intersectsSphere(Jr)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(Wu.x=r.normal.x>0?e.max.x:e.min.x,Wu.y=r.normal.y>0?e.max.y:e.min.y,Wu.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(Wu)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var rc=class extends $i{constructor(e,t,i,r,s,o,a,c,l,u=ts){if(u!==ts&&u!==os)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");i===void 0&&u===ts&&(i=br),i===void 0&&u===os&&(i=ms),super(null,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.image={width:e,height:t},this.magFilter=a!==void 0?a:Vn,this.minFilter=c!==void 0?c:Vn,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},Dn=class{constructor(){this.type="Curve",this.arcLengthDivisions=200}getPoint(){return console.warn("THREE.Curve: .getPoint() not implemented."),null}getPointAt(e,t){let i=this.getUtoTmapping(e);return this.getPoint(i,t)}getPoints(e=5){let t=[];for(let i=0;i<=e;i++)t.push(this.getPoint(i/e));return t}getSpacedPoints(e=5){let t=[];for(let i=0;i<=e;i++)t.push(this.getPointAt(i/e));return t}getLength(){let e=this.getLengths();return e[e.length-1]}getLengths(e=this.arcLengthDivisions){if(this.cacheArcLengths&&this.cacheArcLengths.length===e+1&&!this.needsUpdate)return this.cacheArcLengths;this.needsUpdate=!1;let t=[],i,r=this.getPoint(0),s=0;t.push(0);for(let o=1;o<=e;o++)i=this.getPoint(o/e),s+=i.distanceTo(r),t.push(s),r=i;return this.cacheArcLengths=t,t}updateArcLengths(){this.needsUpdate=!0,this.getLengths()}getUtoTmapping(e,t){let i=this.getLengths(),r=0,s=i.length,o;t?o=t:o=e*i[s-1];let a=0,c=s-1,l;for(;a<=c;)if(r=Math.floor(a+(c-a)/2),l=i[r]-o,l<0)a=r+1;else if(l>0)c=r-1;else{c=r;break}if(r=c,i[r]===o)return r/(s-1);let u=i[r],f=i[r+1]-u,h=(o-u)/f;return(r+h)/(s-1)}getTangent(e,t){let r=e-1e-4,s=e+1e-4;r<0&&(r=0),s>1&&(s=1);let o=this.getPoint(r),a=this.getPoint(s),c=t||(o.isVector2?new fe:new L);return c.copy(a).sub(o).normalize(),c}getTangentAt(e,t){let i=this.getUtoTmapping(e);return this.getTangent(i,t)}computeFrenetFrames(e,t){let i=new L,r=[],s=[],o=[],a=new L,c=new bt;for(let h=0;h<=e;h++){let g=h/e;r[h]=this.getTangentAt(g,new L)}s[0]=new L,o[0]=new L;let l=Number.MAX_VALUE,u=Math.abs(r[0].x),d=Math.abs(r[0].y),f=Math.abs(r[0].z);u<=l&&(l=u,i.set(1,0,0)),d<=l&&(l=d,i.set(0,1,0)),f<=l&&i.set(0,0,1),a.crossVectors(r[0],i).normalize(),s[0].crossVectors(r[0],a),o[0].crossVectors(r[0],s[0]);for(let h=1;h<=e;h++){if(s[h]=s[h-1].clone(),o[h]=o[h-1].clone(),a.crossVectors(r[h-1],r[h]),a.length()>Number.EPSILON){a.normalize();let g=Math.acos(Je(r[h-1].dot(r[h]),-1,1));s[h].applyMatrix4(c.makeRotationAxis(a,g))}o[h].crossVectors(r[h],s[h])}if(t===!0){let h=Math.acos(Je(s[0].dot(s[e]),-1,1));h/=e,r[0].dot(a.crossVectors(s[0],s[e]))>0&&(h=-h);for(let g=1;g<=e;g++)s[g].applyMatrix4(c.makeRotationAxis(r[g],h*g)),o[g].crossVectors(r[g],s[g])}return{tangents:r,normals:s,binormals:o}}clone(){return new this.constructor().copy(this)}copy(e){return this.arcLengthDivisions=e.arcLengthDivisions,this}toJSON(){let e={metadata:{version:4.6,type:"Curve",generator:"Curve.toJSON"}};return e.arcLengthDivisions=this.arcLengthDivisions,e.type=this.type,e}fromJSON(e){return this.arcLengthDivisions=e.arcLengthDivisions,this}},ko=class extends Dn{constructor(e=0,t=0,i=1,r=1,s=0,o=Math.PI*2,a=!1,c=0){super(),this.isEllipseCurve=!0,this.type="EllipseCurve",this.aX=e,this.aY=t,this.xRadius=i,this.yRadius=r,this.aStartAngle=s,this.aEndAngle=o,this.aClockwise=a,this.aRotation=c}getPoint(e,t=new fe){let i=t,r=Math.PI*2,s=this.aEndAngle-this.aStartAngle,o=Math.abs(s)<Number.EPSILON;for(;s<0;)s+=r;for(;s>r;)s-=r;s<Number.EPSILON&&(o?s=0:s=r),this.aClockwise===!0&&!o&&(s===r?s=-r:s=s-r);let a=this.aStartAngle+e*s,c=this.aX+this.xRadius*Math.cos(a),l=this.aY+this.yRadius*Math.sin(a);if(this.aRotation!==0){let u=Math.cos(this.aRotation),d=Math.sin(this.aRotation),f=c-this.aX,h=l-this.aY;c=f*u-h*d+this.aX,l=f*d+h*u+this.aY}return i.set(c,l)}copy(e){return super.copy(e),this.aX=e.aX,this.aY=e.aY,this.xRadius=e.xRadius,this.yRadius=e.yRadius,this.aStartAngle=e.aStartAngle,this.aEndAngle=e.aEndAngle,this.aClockwise=e.aClockwise,this.aRotation=e.aRotation,this}toJSON(){let e=super.toJSON();return e.aX=this.aX,e.aY=this.aY,e.xRadius=this.xRadius,e.yRadius=this.yRadius,e.aStartAngle=this.aStartAngle,e.aEndAngle=this.aEndAngle,e.aClockwise=this.aClockwise,e.aRotation=this.aRotation,e}fromJSON(e){return super.fromJSON(e),this.aX=e.aX,this.aY=e.aY,this.xRadius=e.xRadius,this.yRadius=e.yRadius,this.aStartAngle=e.aStartAngle,this.aEndAngle=e.aEndAngle,this.aClockwise=e.aClockwise,this.aRotation=e.aRotation,this}},ld=class extends ko{constructor(e,t,i,r,s,o){super(e,t,i,i,r,s,o),this.isArcCurve=!0,this.type="ArcCurve"}};function Ug(){let n=0,e=0,t=0,i=0;function r(s,o,a,c){n=s,e=a,t=-3*s+3*o-2*a-c,i=2*s-2*o+a+c}return{initCatmullRom:function(s,o,a,c,l){r(o,a,l*(a-s),l*(c-o))},initNonuniformCatmullRom:function(s,o,a,c,l,u,d){let f=(o-s)/l-(a-s)/(l+u)+(a-o)/u,h=(a-o)/u-(c-o)/(u+d)+(c-a)/d;f*=u,h*=u,r(o,a,f,h)},calc:function(s){let o=s*s,a=o*s;return n+e*s+t*o+i*a}}}var $u=new L,tg=new Ug,ng=new Ug,ig=new Ug,ud=class extends Dn{constructor(e=[],t=!1,i="centripetal",r=.5){super(),this.isCatmullRomCurve3=!0,this.type="CatmullRomCurve3",this.points=e,this.closed=t,this.curveType=i,this.tension=r}getPoint(e,t=new L){let i=t,r=this.points,s=r.length,o=(s-(this.closed?0:1))*e,a=Math.floor(o),c=o-a;this.closed?a+=a>0?0:(Math.floor(Math.abs(a)/s)+1)*s:c===0&&a===s-1&&(a=s-2,c=1);let l,u;this.closed||a>0?l=r[(a-1)%s]:($u.subVectors(r[0],r[1]).add(r[0]),l=$u);let d=r[a%s],f=r[(a+1)%s];if(this.closed||a+2<s?u=r[(a+2)%s]:($u.subVectors(r[s-1],r[s-2]).add(r[s-1]),u=$u),this.curveType==="centripetal"||this.curveType==="chordal"){let h=this.curveType==="chordal"?.5:.25,g=Math.pow(l.distanceToSquared(d),h),v=Math.pow(d.distanceToSquared(f),h),m=Math.pow(f.distanceToSquared(u),h);v<1e-4&&(v=1),g<1e-4&&(g=v),m<1e-4&&(m=v),tg.initNonuniformCatmullRom(l.x,d.x,f.x,u.x,g,v,m),ng.initNonuniformCatmullRom(l.y,d.y,f.y,u.y,g,v,m),ig.initNonuniformCatmullRom(l.z,d.z,f.z,u.z,g,v,m)}else this.curveType==="catmullrom"&&(tg.initCatmullRom(l.x,d.x,f.x,u.x,this.tension),ng.initCatmullRom(l.y,d.y,f.y,u.y,this.tension),ig.initCatmullRom(l.z,d.z,f.z,u.z,this.tension));return i.set(tg.calc(c),ng.calc(c),ig.calc(c)),i}copy(e){super.copy(e),this.points=[];for(let t=0,i=e.points.length;t<i;t++){let r=e.points[t];this.points.push(r.clone())}return this.closed=e.closed,this.curveType=e.curveType,this.tension=e.tension,this}toJSON(){let e=super.toJSON();e.points=[];for(let t=0,i=this.points.length;t<i;t++){let r=this.points[t];e.points.push(r.toArray())}return e.closed=this.closed,e.curveType=this.curveType,e.tension=this.tension,e}fromJSON(e){super.fromJSON(e),this.points=[];for(let t=0,i=e.points.length;t<i;t++){let r=e.points[t];this.points.push(new L().fromArray(r))}return this.closed=e.closed,this.curveType=e.curveType,this.tension=e.tension,this}};function mM(n,e,t,i,r){let s=(i-e)*.5,o=(r-t)*.5,a=n*n,c=n*a;return(2*t-2*i+s+o)*c+(-3*t+3*i-2*s-o)*a+s*n+t}function nP(n,e){let t=1-n;return t*t*e}function iP(n,e){return 2*(1-n)*n*e}function rP(n,e){return n*n*e}function Wa(n,e,t,i){return nP(n,e)+iP(n,t)+rP(n,i)}function sP(n,e){let t=1-n;return t*t*t*e}function oP(n,e){let t=1-n;return 3*t*t*n*e}function aP(n,e){return 3*(1-n)*n*n*e}function cP(n,e){return n*n*n*e}function $a(n,e,t,i,r){return sP(n,e)+oP(n,t)+aP(n,i)+cP(n,r)}var sc=class extends Dn{constructor(e=new fe,t=new fe,i=new fe,r=new fe){super(),this.isCubicBezierCurve=!0,this.type="CubicBezierCurve",this.v0=e,this.v1=t,this.v2=i,this.v3=r}getPoint(e,t=new fe){let i=t,r=this.v0,s=this.v1,o=this.v2,a=this.v3;return i.set($a(e,r.x,s.x,o.x,a.x),$a(e,r.y,s.y,o.y,a.y)),i}copy(e){return super.copy(e),this.v0.copy(e.v0),this.v1.copy(e.v1),this.v2.copy(e.v2),this.v3.copy(e.v3),this}toJSON(){let e=super.toJSON();return e.v0=this.v0.toArray(),e.v1=this.v1.toArray(),e.v2=this.v2.toArray(),e.v3=this.v3.toArray(),e}fromJSON(e){return super.fromJSON(e),this.v0.fromArray(e.v0),this.v1.fromArray(e.v1),this.v2.fromArray(e.v2),this.v3.fromArray(e.v3),this}},dd=class extends Dn{constructor(e=new L,t=new L,i=new L,r=new L){super(),this.isCubicBezierCurve3=!0,this.type="CubicBezierCurve3",this.v0=e,this.v1=t,this.v2=i,this.v3=r}getPoint(e,t=new L){let i=t,r=this.v0,s=this.v1,o=this.v2,a=this.v3;return i.set($a(e,r.x,s.x,o.x,a.x),$a(e,r.y,s.y,o.y,a.y),$a(e,r.z,s.z,o.z,a.z)),i}copy(e){return super.copy(e),this.v0.copy(e.v0),this.v1.copy(e.v1),this.v2.copy(e.v2),this.v3.copy(e.v3),this}toJSON(){let e=super.toJSON();return e.v0=this.v0.toArray(),e.v1=this.v1.toArray(),e.v2=this.v2.toArray(),e.v3=this.v3.toArray(),e}fromJSON(e){return super.fromJSON(e),this.v0.fromArray(e.v0),this.v1.fromArray(e.v1),this.v2.fromArray(e.v2),this.v3.fromArray(e.v3),this}},oc=class extends Dn{constructor(e=new fe,t=new fe){super(),this.isLineCurve=!0,this.type="LineCurve",this.v1=e,this.v2=t}getPoint(e,t=new fe){let i=t;return e===1?i.copy(this.v2):(i.copy(this.v2).sub(this.v1),i.multiplyScalar(e).add(this.v1)),i}getPointAt(e,t){return this.getPoint(e,t)}getTangent(e,t=new fe){return t.subVectors(this.v2,this.v1).normalize()}getTangentAt(e,t){return this.getTangent(e,t)}copy(e){return super.copy(e),this.v1.copy(e.v1),this.v2.copy(e.v2),this}toJSON(){let e=super.toJSON();return e.v1=this.v1.toArray(),e.v2=this.v2.toArray(),e}fromJSON(e){return super.fromJSON(e),this.v1.fromArray(e.v1),this.v2.fromArray(e.v2),this}},fd=class extends Dn{constructor(e=new L,t=new L){super(),this.isLineCurve3=!0,this.type="LineCurve3",this.v1=e,this.v2=t}getPoint(e,t=new L){let i=t;return e===1?i.copy(this.v2):(i.copy(this.v2).sub(this.v1),i.multiplyScalar(e).add(this.v1)),i}getPointAt(e,t){return this.getPoint(e,t)}getTangent(e,t=new L){return t.subVectors(this.v2,this.v1).normalize()}getTangentAt(e,t){return this.getTangent(e,t)}copy(e){return super.copy(e),this.v1.copy(e.v1),this.v2.copy(e.v2),this}toJSON(){let e=super.toJSON();return e.v1=this.v1.toArray(),e.v2=this.v2.toArray(),e}fromJSON(e){return super.fromJSON(e),this.v1.fromArray(e.v1),this.v2.fromArray(e.v2),this}},ac=class extends Dn{constructor(e=new fe,t=new fe,i=new fe){super(),this.isQuadraticBezierCurve=!0,this.type="QuadraticBezierCurve",this.v0=e,this.v1=t,this.v2=i}getPoint(e,t=new fe){let i=t,r=this.v0,s=this.v1,o=this.v2;return i.set(Wa(e,r.x,s.x,o.x),Wa(e,r.y,s.y,o.y)),i}copy(e){return super.copy(e),this.v0.copy(e.v0),this.v1.copy(e.v1),this.v2.copy(e.v2),this}toJSON(){let e=super.toJSON();return e.v0=this.v0.toArray(),e.v1=this.v1.toArray(),e.v2=this.v2.toArray(),e}fromJSON(e){return super.fromJSON(e),this.v0.fromArray(e.v0),this.v1.fromArray(e.v1),this.v2.fromArray(e.v2),this}},hd=class extends Dn{constructor(e=new L,t=new L,i=new L){super(),this.isQuadraticBezierCurve3=!0,this.type="QuadraticBezierCurve3",this.v0=e,this.v1=t,this.v2=i}getPoint(e,t=new L){let i=t,r=this.v0,s=this.v1,o=this.v2;return i.set(Wa(e,r.x,s.x,o.x),Wa(e,r.y,s.y,o.y),Wa(e,r.z,s.z,o.z)),i}copy(e){return super.copy(e),this.v0.copy(e.v0),this.v1.copy(e.v1),this.v2.copy(e.v2),this}toJSON(){let e=super.toJSON();return e.v0=this.v0.toArray(),e.v1=this.v1.toArray(),e.v2=this.v2.toArray(),e}fromJSON(e){return super.fromJSON(e),this.v0.fromArray(e.v0),this.v1.fromArray(e.v1),this.v2.fromArray(e.v2),this}},cc=class extends Dn{constructor(e=[]){super(),this.isSplineCurve=!0,this.type="SplineCurve",this.points=e}getPoint(e,t=new fe){let i=t,r=this.points,s=(r.length-1)*e,o=Math.floor(s),a=s-o,c=r[o===0?o:o-1],l=r[o],u=r[o>r.length-2?r.length-1:o+1],d=r[o>r.length-3?r.length-1:o+2];return i.set(mM(a,c.x,l.x,u.x,d.x),mM(a,c.y,l.y,u.y,d.y)),i}copy(e){super.copy(e),this.points=[];for(let t=0,i=e.points.length;t<i;t++){let r=e.points[t];this.points.push(r.clone())}return this}toJSON(){let e=super.toJSON();e.points=[];for(let t=0,i=this.points.length;t<i;t++){let r=this.points[t];e.points.push(r.toArray())}return e}fromJSON(e){super.fromJSON(e),this.points=[];for(let t=0,i=e.points.length;t<i;t++){let r=e.points[t];this.points.push(new fe().fromArray(r))}return this}},dg=Object.freeze({__proto__:null,ArcCurve:ld,CatmullRomCurve3:ud,CubicBezierCurve:sc,CubicBezierCurve3:dd,EllipseCurve:ko,LineCurve:oc,LineCurve3:fd,QuadraticBezierCurve:ac,QuadraticBezierCurve3:hd,SplineCurve:cc}),pd=class extends Dn{constructor(){super(),this.type="CurvePath",this.curves=[],this.autoClose=!1}add(e){this.curves.push(e)}closePath(){let e=this.curves[0].getPoint(0),t=this.curves[this.curves.length-1].getPoint(1);if(!e.equals(t)){let i=e.isVector2===!0?"LineCurve":"LineCurve3";this.curves.push(new dg[i](t,e))}return this}getPoint(e,t){let i=e*this.getLength(),r=this.getCurveLengths(),s=0;for(;s<r.length;){if(r[s]>=i){let o=r[s]-i,a=this.curves[s],c=a.getLength(),l=c===0?0:1-o/c;return a.getPointAt(l,t)}s++}return null}getLength(){let e=this.getCurveLengths();return e[e.length-1]}updateArcLengths(){this.needsUpdate=!0,this.cacheLengths=null,this.getCurveLengths()}getCurveLengths(){if(this.cacheLengths&&this.cacheLengths.length===this.curves.length)return this.cacheLengths;let e=[],t=0;for(let i=0,r=this.curves.length;i<r;i++)t+=this.curves[i].getLength(),e.push(t);return this.cacheLengths=e,e}getSpacedPoints(e=40){let t=[];for(let i=0;i<=e;i++)t.push(this.getPoint(i/e));return this.autoClose&&t.push(t[0]),t}getPoints(e=12){let t=[],i;for(let r=0,s=this.curves;r<s.length;r++){let o=s[r],a=o.isEllipseCurve?e*2:o.isLineCurve||o.isLineCurve3?1:o.isSplineCurve?e*o.points.length:e,c=o.getPoints(a);for(let l=0;l<c.length;l++){let u=c[l];i&&i.equals(u)||(t.push(u),i=u)}}return this.autoClose&&t.length>1&&!t[t.length-1].equals(t[0])&&t.push(t[0]),t}copy(e){super.copy(e),this.curves=[];for(let t=0,i=e.curves.length;t<i;t++){let r=e.curves[t];this.curves.push(r.clone())}return this.autoClose=e.autoClose,this}toJSON(){let e=super.toJSON();e.autoClose=this.autoClose,e.curves=[];for(let t=0,i=this.curves.length;t<i;t++){let r=this.curves[t];e.curves.push(r.toJSON())}return e}fromJSON(e){super.fromJSON(e),this.autoClose=e.autoClose,this.curves=[];for(let t=0,i=e.curves.length;t<i;t++){let r=e.curves[t];this.curves.push(new dg[r.type]().fromJSON(r))}return this}},Uo=class extends pd{constructor(e){super(),this.type="Path",this.currentPoint=new fe,e&&this.setFromPoints(e)}setFromPoints(e){this.moveTo(e[0].x,e[0].y);for(let t=1,i=e.length;t<i;t++)this.lineTo(e[t].x,e[t].y);return this}moveTo(e,t){return this.currentPoint.set(e,t),this}lineTo(e,t){let i=new oc(this.currentPoint.clone(),new fe(e,t));return this.curves.push(i),this.currentPoint.set(e,t),this}quadraticCurveTo(e,t,i,r){let s=new ac(this.currentPoint.clone(),new fe(e,t),new fe(i,r));return this.curves.push(s),this.currentPoint.set(i,r),this}bezierCurveTo(e,t,i,r,s,o){let a=new sc(this.currentPoint.clone(),new fe(e,t),new fe(i,r),new fe(s,o));return this.curves.push(a),this.currentPoint.set(s,o),this}splineThru(e){let t=[this.currentPoint.clone()].concat(e),i=new cc(t);return this.curves.push(i),this.currentPoint.copy(e[e.length-1]),this}arc(e,t,i,r,s,o){let a=this.currentPoint.x,c=this.currentPoint.y;return this.absarc(e+a,t+c,i,r,s,o),this}absarc(e,t,i,r,s,o){return this.absellipse(e,t,i,i,r,s,o),this}ellipse(e,t,i,r,s,o,a,c){let l=this.currentPoint.x,u=this.currentPoint.y;return this.absellipse(e+l,t+u,i,r,s,o,a,c),this}absellipse(e,t,i,r,s,o,a,c){let l=new ko(e,t,i,r,s,o,a,c);if(this.curves.length>0){let d=l.getPoint(0);d.equals(this.currentPoint)||this.lineTo(d.x,d.y)}this.curves.push(l);let u=l.getPoint(1);return this.currentPoint.copy(u),this}copy(e){return super.copy(e),this.currentPoint.copy(e.currentPoint),this}toJSON(){let e=super.toJSON();return e.currentPoint=this.currentPoint.toArray(),e}fromJSON(e){return super.fromJSON(e),this.currentPoint.fromArray(e.currentPoint),this}};var ns=class extends Uo{constructor(e){super(e),this.uuid=Ho(),this.type="Shape",this.holes=[]}getPointsHoles(e){let t=[];for(let i=0,r=this.holes.length;i<r;i++)t[i]=this.holes[i].getPoints(e);return t}extractPoints(e){return{shape:this.getPoints(e),holes:this.getPointsHoles(e)}}copy(e){super.copy(e),this.holes=[];for(let t=0,i=e.holes.length;t<i;t++){let r=e.holes[t];this.holes.push(r.clone())}return this}toJSON(){let e=super.toJSON();e.uuid=this.uuid,e.holes=[];for(let t=0,i=this.holes.length;t<i;t++){let r=this.holes[t];e.holes.push(r.toJSON())}return e}fromJSON(e){super.fromJSON(e),this.uuid=e.uuid,this.holes=[];for(let t=0,i=e.holes.length;t<i;t++){let r=e.holes[t];this.holes.push(new Uo().fromJSON(r))}return this}},lP={triangulate:function(n,e,t=2){let i=e&&e.length,r=i?e[0]*t:n.length,s=pS(n,0,r,t,!0),o=[];if(!s||s.next===s.prev)return o;let a,c,l,u,d,f,h;if(i&&(s=pP(n,e,s,t)),n.length>80*t){a=l=n[0],c=u=n[1];for(let g=t;g<r;g+=t)d=n[g],f=n[g+1],d<a&&(a=d),f<c&&(c=f),d>l&&(l=d),f>u&&(u=f);h=Math.max(l-a,u-c),h=h!==0?32767/h:0}return lc(s,o,t,a,c,h,0),o}};function pS(n,e,t,i,r){let s,o;if(r===bP(n,e,t,i)>0)for(s=e;s<t;s+=i)o=gM(s,n[s],n[s+1],o);else for(s=t-i;s>=e;s-=i)o=gM(s,n[s],n[s+1],o);return o&&yf(o,o.next)&&(dc(o),o=o.next),o}function cs(n,e){if(!n)return n;e||(e=n);let t=n,i;do if(i=!1,!t.steiner&&(yf(t,t.next)||wt(t.prev,t,t.next)===0)){if(dc(t),t=e=t.prev,t===t.next)break;i=!0}else t=t.next;while(i||t!==e);return e}function lc(n,e,t,i,r,s,o){if(!n)return;!o&&s&&_P(n,i,r,s);let a=n,c,l;for(;n.prev!==n.next;){if(c=n.prev,l=n.next,s?dP(n,i,r,s):uP(n)){e.push(c.i/t|0),e.push(n.i/t|0),e.push(l.i/t|0),dc(n),n=l.next,a=l.next;continue}if(n=l,n===a){o?o===1?(n=fP(cs(n),e,t),lc(n,e,t,i,r,s,2)):o===2&&hP(n,e,t,i,r,s):lc(cs(n),e,t,i,r,s,1);break}}}function uP(n){let e=n.prev,t=n,i=n.next;if(wt(e,t,i)>=0)return!1;let r=e.x,s=t.x,o=i.x,a=e.y,c=t.y,l=i.y,u=r<s?r<o?r:o:s<o?s:o,d=a<c?a<l?a:l:c<l?c:l,f=r>s?r>o?r:o:s>o?s:o,h=a>c?a>l?a:l:c>l?c:l,g=i.next;for(;g!==e;){if(g.x>=u&&g.x<=f&&g.y>=d&&g.y<=h&&Io(r,a,s,c,o,l,g.x,g.y)&&wt(g.prev,g,g.next)>=0)return!1;g=g.next}return!0}function dP(n,e,t,i){let r=n.prev,s=n,o=n.next;if(wt(r,s,o)>=0)return!1;let a=r.x,c=s.x,l=o.x,u=r.y,d=s.y,f=o.y,h=a<c?a<l?a:l:c<l?c:l,g=u<d?u<f?u:f:d<f?d:f,v=a>c?a>l?a:l:c>l?c:l,m=u>d?u>f?u:f:d>f?d:f,p=fg(h,g,e,t,i),T=fg(v,m,e,t,i),M=n.prevZ,E=n.nextZ;for(;M&&M.z>=p&&E&&E.z<=T;){if(M.x>=h&&M.x<=v&&M.y>=g&&M.y<=m&&M!==r&&M!==o&&Io(a,u,c,d,l,f,M.x,M.y)&&wt(M.prev,M,M.next)>=0||(M=M.prevZ,E.x>=h&&E.x<=v&&E.y>=g&&E.y<=m&&E!==r&&E!==o&&Io(a,u,c,d,l,f,E.x,E.y)&&wt(E.prev,E,E.next)>=0))return!1;E=E.nextZ}for(;M&&M.z>=p;){if(M.x>=h&&M.x<=v&&M.y>=g&&M.y<=m&&M!==r&&M!==o&&Io(a,u,c,d,l,f,M.x,M.y)&&wt(M.prev,M,M.next)>=0)return!1;M=M.prevZ}for(;E&&E.z<=T;){if(E.x>=h&&E.x<=v&&E.y>=g&&E.y<=m&&E!==r&&E!==o&&Io(a,u,c,d,l,f,E.x,E.y)&&wt(E.prev,E,E.next)>=0)return!1;E=E.nextZ}return!0}function fP(n,e,t){let i=n;do{let r=i.prev,s=i.next.next;!yf(r,s)&&mS(r,i,i.next,s)&&uc(r,s)&&uc(s,r)&&(e.push(r.i/t|0),e.push(i.i/t|0),e.push(s.i/t|0),dc(i),dc(i.next),i=n=s),i=i.next}while(i!==n);return cs(i)}function hP(n,e,t,i,r,s){let o=n;do{let a=o.next.next;for(;a!==o.prev;){if(o.i!==a.i&&MP(o,a)){let c=gS(o,a);o=cs(o,o.next),c=cs(c,c.next),lc(o,e,t,i,r,s,0),lc(c,e,t,i,r,s,0);return}a=a.next}o=o.next}while(o!==n)}function pP(n,e,t,i){let r=[],s,o,a,c,l;for(s=0,o=e.length;s<o;s++)a=e[s]*i,c=s<o-1?e[s+1]*i:n.length,l=pS(n,a,c,i,!1),l===l.next&&(l.steiner=!0),r.push(EP(l));for(r.sort(mP),s=0;s<r.length;s++)t=gP(r[s],t);return t}function mP(n,e){return n.x-e.x}function gP(n,e){let t=vP(n,e);if(!t)return e;let i=gS(t,n);return cs(i,i.next),cs(t,t.next)}function vP(n,e){let t=e,i=-1/0,r,s=n.x,o=n.y;do{if(o<=t.y&&o>=t.next.y&&t.next.y!==t.y){let f=t.x+(o-t.y)*(t.next.x-t.x)/(t.next.y-t.y);if(f<=s&&f>i&&(i=f,r=t.x<t.next.x?t:t.next,f===s))return r}t=t.next}while(t!==e);if(!r)return null;let a=r,c=r.x,l=r.y,u=1/0,d;t=r;do s>=t.x&&t.x>=c&&s!==t.x&&Io(o<l?s:i,o,c,l,o<l?i:s,o,t.x,t.y)&&(d=Math.abs(o-t.y)/(s-t.x),uc(t,n)&&(d<u||d===u&&(t.x>r.x||t.x===r.x&&yP(r,t)))&&(r=t,u=d)),t=t.next;while(t!==a);return r}function yP(n,e){return wt(n.prev,n,e.prev)<0&&wt(e.next,n,n.next)<0}function _P(n,e,t,i){let r=n;do r.z===0&&(r.z=fg(r.x,r.y,e,t,i)),r.prevZ=r.prev,r.nextZ=r.next,r=r.next;while(r!==n);r.prevZ.nextZ=null,r.prevZ=null,xP(r)}function xP(n){let e,t,i,r,s,o,a,c,l=1;do{for(t=n,n=null,s=null,o=0;t;){for(o++,i=t,a=0,e=0;e<l&&(a++,i=i.nextZ,!!i);e++);for(c=l;a>0||c>0&&i;)a!==0&&(c===0||!i||t.z<=i.z)?(r=t,t=t.nextZ,a--):(r=i,i=i.nextZ,c--),s?s.nextZ=r:n=r,r.prevZ=s,s=r;t=i}s.nextZ=null,l*=2}while(o>1);return n}function fg(n,e,t,i,r){return n=(n-t)*r|0,e=(e-i)*r|0,n=(n|n<<8)&16711935,n=(n|n<<4)&252645135,n=(n|n<<2)&858993459,n=(n|n<<1)&1431655765,e=(e|e<<8)&16711935,e=(e|e<<4)&252645135,e=(e|e<<2)&858993459,e=(e|e<<1)&1431655765,n|e<<1}function EP(n){let e=n,t=n;do(e.x<t.x||e.x===t.x&&e.y<t.y)&&(t=e),e=e.next;while(e!==n);return t}function Io(n,e,t,i,r,s,o,a){return(r-o)*(e-a)>=(n-o)*(s-a)&&(n-o)*(i-a)>=(t-o)*(e-a)&&(t-o)*(s-a)>=(r-o)*(i-a)}function MP(n,e){return n.next.i!==e.i&&n.prev.i!==e.i&&!SP(n,e)&&(uc(n,e)&&uc(e,n)&&wP(n,e)&&(wt(n.prev,n,e.prev)||wt(n,e.prev,e))||yf(n,e)&&wt(n.prev,n,n.next)>0&&wt(e.prev,e,e.next)>0)}function wt(n,e,t){return(e.y-n.y)*(t.x-e.x)-(e.x-n.x)*(t.y-e.y)}function yf(n,e){return n.x===e.x&&n.y===e.y}function mS(n,e,t,i){let r=Xu(wt(n,e,t)),s=Xu(wt(n,e,i)),o=Xu(wt(t,i,n)),a=Xu(wt(t,i,e));return!!(r!==s&&o!==a||r===0&&qu(n,t,e)||s===0&&qu(n,i,e)||o===0&&qu(t,n,i)||a===0&&qu(t,e,i))}function qu(n,e,t){return e.x<=Math.max(n.x,t.x)&&e.x>=Math.min(n.x,t.x)&&e.y<=Math.max(n.y,t.y)&&e.y>=Math.min(n.y,t.y)}function Xu(n){return n>0?1:n<0?-1:0}function SP(n,e){let t=n;do{if(t.i!==n.i&&t.next.i!==n.i&&t.i!==e.i&&t.next.i!==e.i&&mS(t,t.next,n,e))return!0;t=t.next}while(t!==n);return!1}function uc(n,e){return wt(n.prev,n,n.next)<0?wt(n,e,n.next)>=0&&wt(n,n.prev,e)>=0:wt(n,e,n.prev)<0||wt(n,n.next,e)<0}function wP(n,e){let t=n,i=!1,r=(n.x+e.x)/2,s=(n.y+e.y)/2;do t.y>s!=t.next.y>s&&t.next.y!==t.y&&r<(t.next.x-t.x)*(s-t.y)/(t.next.y-t.y)+t.x&&(i=!i),t=t.next;while(t!==n);return i}function gS(n,e){let t=new hg(n.i,n.x,n.y),i=new hg(e.i,e.x,e.y),r=n.next,s=e.prev;return n.next=e,e.prev=n,t.next=r,r.prev=t,i.next=t,t.prev=i,s.next=i,i.prev=s,i}function gM(n,e,t,i){let r=new hg(n,e,t);return i?(r.next=i.next,r.prev=i,i.next.prev=r,i.next=r):(r.prev=r,r.next=r),r}function dc(n){n.next.prev=n.prev,n.prev.next=n.next,n.prevZ&&(n.prevZ.nextZ=n.nextZ),n.nextZ&&(n.nextZ.prevZ=n.prevZ)}function hg(n,e,t){this.i=n,this.x=e,this.y=t,this.prev=null,this.next=null,this.z=0,this.prevZ=null,this.nextZ=null,this.steiner=!1}function bP(n,e,t,i){let r=0;for(let s=e,o=t-i;s<t;s+=i)r+=(n[o]-n[s])*(n[s+1]+n[o+1]),o=s;return r}var is=class n{static area(e){let t=e.length,i=0;for(let r=t-1,s=0;s<t;r=s++)i+=e[r].x*e[s].y-e[s].x*e[r].y;return i*.5}static isClockWise(e){return n.area(e)<0}static triangulateShape(e,t){let i=[],r=[],s=[];vM(e),yM(i,e);let o=e.length;t.forEach(vM);for(let c=0;c<t.length;c++)r.push(o),o+=t[c].length,yM(i,t[c]);let a=lP.triangulate(i,r);for(let c=0;c<a.length;c+=3)s.push(a.slice(c,c+3));return s}};function vM(n){let e=n.length;e>2&&n[e-1].equals(n[0])&&n.pop()}function yM(n,e){for(let t=0;t<e.length;t++)n.push(e[t].x),n.push(e[t].y)}var fc=class n extends fi{constructor(e=new ns([new fe(.5,.5),new fe(-.5,.5),new fe(-.5,-.5),new fe(.5,-.5)]),t={}){super(),this.type="ExtrudeGeometry",this.parameters={shapes:e,options:t},e=Array.isArray(e)?e:[e];let i=this,r=[],s=[];for(let a=0,c=e.length;a<c;a++){let l=e[a];o(l)}this.setAttribute("position",new rn(r,3)),this.setAttribute("uv",new rn(s,2)),this.computeVertexNormals();function o(a){let c=[],l=t.curveSegments!==void 0?t.curveSegments:12,u=t.steps!==void 0?t.steps:1,d=t.depth!==void 0?t.depth:1,f=t.bevelEnabled!==void 0?t.bevelEnabled:!0,h=t.bevelThickness!==void 0?t.bevelThickness:.2,g=t.bevelSize!==void 0?t.bevelSize:h-.1,v=t.bevelOffset!==void 0?t.bevelOffset:0,m=t.bevelSegments!==void 0?t.bevelSegments:3,p=t.extrudePath,T=t.UVGenerator!==void 0?t.UVGenerator:TP,M,E=!1,I,D,C,N;p&&(M=p.getSpacedPoints(u),E=!0,f=!1,I=p.computeFrenetFrames(u,!1),D=new L,C=new L,N=new L),f||(m=0,h=0,g=0,v=0);let w=a.extractPoints(l),x=w.shape,A=w.holes;if(!is.isClockWise(x)){x=x.reverse();for(let ee=0,J=A.length;ee<J;ee++){let b=A[ee];is.isClockWise(b)&&(A[ee]=b.reverse())}}let B=is.triangulateShape(x,A),q=x;for(let ee=0,J=A.length;ee<J;ee++){let b=A[ee];x=x.concat(b)}function X(ee,J,b){return J||console.error("THREE.ExtrudeGeometry: vec does not exist"),ee.clone().addScaledVector(J,b)}let H=x.length,Z=B.length;function V(ee,J,b){let Ce,te,ye,re=ee.x-J.x,Fe=ee.y-J.y,he=b.x-ee.x,S=b.y-ee.y,y=re*re+Fe*Fe,F=re*S-Fe*he;if(Math.abs(F)>Number.EPSILON){let G=Math.sqrt(y),K=Math.sqrt(he*he+S*S),j=J.x-Fe/G,Te=J.y+re/G,le=b.x-S/K,ve=b.y+he/K,Qe=((le-j)*S-(ve-Te)*he)/(re*S-Fe*he);Ce=j+re*Qe-ee.x,te=Te+Fe*Qe-ee.y;let ie=Ce*Ce+te*te;if(ie<=2)return new fe(Ce,te);ye=Math.sqrt(ie/2)}else{let G=!1;re>Number.EPSILON?he>Number.EPSILON&&(G=!0):re<-Number.EPSILON?he<-Number.EPSILON&&(G=!0):Math.sign(Fe)===Math.sign(S)&&(G=!0),G?(Ce=-Fe,te=re,ye=Math.sqrt(y)):(Ce=re,te=Fe,ye=Math.sqrt(y/2))}return new fe(Ce/ye,te/ye)}let ae=[];for(let ee=0,J=q.length,b=J-1,Ce=ee+1;ee<J;ee++,b++,Ce++)b===J&&(b=0),Ce===J&&(Ce=0),ae[ee]=V(q[ee],q[b],q[Ce]);let ge=[],Se,Ge=ae.concat();for(let ee=0,J=A.length;ee<J;ee++){let b=A[ee];Se=[];for(let Ce=0,te=b.length,ye=te-1,re=Ce+1;Ce<te;Ce++,ye++,re++)ye===te&&(ye=0),re===te&&(re=0),Se[Ce]=V(b[Ce],b[ye],b[re]);ge.push(Se),Ge=Ge.concat(Se)}for(let ee=0;ee<m;ee++){let J=ee/m,b=h*Math.cos(J*Math.PI/2),Ce=g*Math.sin(J*Math.PI/2)+v;for(let te=0,ye=q.length;te<ye;te++){let re=X(q[te],ae[te],Ce);oe(re.x,re.y,-b)}for(let te=0,ye=A.length;te<ye;te++){let re=A[te];Se=ge[te];for(let Fe=0,he=re.length;Fe<he;Fe++){let S=X(re[Fe],Se[Fe],Ce);oe(S.x,S.y,-b)}}}let dt=g+v;for(let ee=0;ee<H;ee++){let J=f?X(x[ee],Ge[ee],dt):x[ee];E?(C.copy(I.normals[0]).multiplyScalar(J.x),D.copy(I.binormals[0]).multiplyScalar(J.y),N.copy(M[0]).add(C).add(D),oe(N.x,N.y,N.z)):oe(J.x,J.y,0)}for(let ee=1;ee<=u;ee++)for(let J=0;J<H;J++){let b=f?X(x[J],Ge[J],dt):x[J];E?(C.copy(I.normals[ee]).multiplyScalar(b.x),D.copy(I.binormals[ee]).multiplyScalar(b.y),N.copy(M[ee]).add(C).add(D),oe(N.x,N.y,N.z)):oe(b.x,b.y,d/u*ee)}for(let ee=m-1;ee>=0;ee--){let J=ee/m,b=h*Math.cos(J*Math.PI/2),Ce=g*Math.sin(J*Math.PI/2)+v;for(let te=0,ye=q.length;te<ye;te++){let re=X(q[te],ae[te],Ce);oe(re.x,re.y,d+b)}for(let te=0,ye=A.length;te<ye;te++){let re=A[te];Se=ge[te];for(let Fe=0,he=re.length;Fe<he;Fe++){let S=X(re[Fe],Se[Fe],Ce);E?oe(S.x,S.y+M[u-1].y,M[u-1].x+b):oe(S.x,S.y,d+b)}}}W(),se();function W(){let ee=r.length/3;if(f){let J=0,b=H*J;for(let Ce=0;Ce<Z;Ce++){let te=B[Ce];Ie(te[2]+b,te[1]+b,te[0]+b)}J=u+m*2,b=H*J;for(let Ce=0;Ce<Z;Ce++){let te=B[Ce];Ie(te[0]+b,te[1]+b,te[2]+b)}}else{for(let J=0;J<Z;J++){let b=B[J];Ie(b[2],b[1],b[0])}for(let J=0;J<Z;J++){let b=B[J];Ie(b[0]+H*u,b[1]+H*u,b[2]+H*u)}}i.addGroup(ee,r.length/3-ee,0)}function se(){let ee=r.length/3,J=0;we(q,J),J+=q.length;for(let b=0,Ce=A.length;b<Ce;b++){let te=A[b];we(te,J),J+=te.length}i.addGroup(ee,r.length/3-ee,1)}function we(ee,J){let b=ee.length;for(;--b>=0;){let Ce=b,te=b-1;te<0&&(te=ee.length-1);for(let ye=0,re=u+m*2;ye<re;ye++){let Fe=H*ye,he=H*(ye+1),S=J+Ce+Fe,y=J+te+Fe,F=J+te+he,G=J+Ce+he;ot(S,y,F,G)}}}function oe(ee,J,b){c.push(ee),c.push(J),c.push(b)}function Ie(ee,J,b){De(ee),De(J),De(b);let Ce=r.length/3,te=T.generateTopUV(i,r,Ce-3,Ce-2,Ce-1);ut(te[0]),ut(te[1]),ut(te[2])}function ot(ee,J,b,Ce){De(ee),De(J),De(Ce),De(J),De(b),De(Ce);let te=r.length/3,ye=T.generateSideWallUV(i,r,te-6,te-3,te-2,te-1);ut(ye[0]),ut(ye[1]),ut(ye[3]),ut(ye[1]),ut(ye[2]),ut(ye[3])}function De(ee){r.push(c[ee*3+0]),r.push(c[ee*3+1]),r.push(c[ee*3+2])}function ut(ee){s.push(ee.x),s.push(ee.y)}}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}toJSON(){let e=super.toJSON(),t=this.parameters.shapes,i=this.parameters.options;return CP(t,i,e)}static fromJSON(e,t){let i=[];for(let s=0,o=e.shapes.length;s<o;s++){let a=t[e.shapes[s]];i.push(a)}let r=e.options.extrudePath;return r!==void 0&&(e.options.extrudePath=new dg[r.type]().fromJSON(r)),new n(i,e.options)}},TP={generateTopUV:function(n,e,t,i,r){let s=e[t*3],o=e[t*3+1],a=e[i*3],c=e[i*3+1],l=e[r*3],u=e[r*3+1];return[new fe(s,o),new fe(a,c),new fe(l,u)]},generateSideWallUV:function(n,e,t,i,r,s){let o=e[t*3],a=e[t*3+1],c=e[t*3+2],l=e[i*3],u=e[i*3+1],d=e[i*3+2],f=e[r*3],h=e[r*3+1],g=e[r*3+2],v=e[s*3],m=e[s*3+1],p=e[s*3+2];return Math.abs(a-u)<Math.abs(o-l)?[new fe(o,1-c),new fe(l,1-d),new fe(f,1-g),new fe(v,1-p)]:[new fe(a,1-c),new fe(u,1-d),new fe(h,1-g),new fe(m,1-p)]}};function CP(n,e,t){if(t.shapes=[],Array.isArray(n))for(let i=0,r=n.length;i<r;i++){let s=n[i];t.shapes.push(s.uuid)}else t.shapes.push(n.uuid);return t.options=Object.assign({},e),e.extrudePath!==void 0&&(t.options.extrudePath=e.extrudePath.toJSON()),t}var ls=class n extends fi{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,d=e/a,f=t/c,h=[],g=[],v=[],m=[];for(let p=0;p<u;p++){let T=p*f-o;for(let M=0;M<l;M++){let E=M*d-s;g.push(E,-T,0),v.push(0,0,1),m.push(M/a),m.push(1-p/c)}}for(let p=0;p<c;p++)for(let T=0;T<a;T++){let M=T+l*p,E=T+l*(p+1),I=T+1+l*(p+1),D=T+1+l*p;h.push(M,E,D),h.push(E,I,D)}this.setIndex(h),this.setAttribute("position",new rn(g,3)),this.setAttribute("normal",new rn(v,3)),this.setAttribute("uv",new rn(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};var us=class n extends fi{constructor(e=1,t=32,i=16,r=0,s=Math.PI*2,o=0,a=Math.PI){super(),this.type="SphereGeometry",this.parameters={radius:e,widthSegments:t,heightSegments:i,phiStart:r,phiLength:s,thetaStart:o,thetaLength:a},t=Math.max(3,Math.floor(t)),i=Math.max(2,Math.floor(i));let c=Math.min(o+a,Math.PI),l=0,u=[],d=new L,f=new L,h=[],g=[],v=[],m=[];for(let p=0;p<=i;p++){let T=[],M=p/i,E=0;p===0&&o===0?E=.5/t:p===i&&c===Math.PI&&(E=-.5/t);for(let I=0;I<=t;I++){let D=I/t;d.x=-e*Math.cos(r+D*s)*Math.sin(o+M*a),d.y=e*Math.cos(o+M*a),d.z=e*Math.sin(r+D*s)*Math.sin(o+M*a),g.push(d.x,d.y,d.z),f.copy(d).normalize(),v.push(f.x,f.y,f.z),m.push(D+E,1-M),T.push(l++)}u.push(T)}for(let p=0;p<i;p++)for(let T=0;T<t;T++){let M=u[p][T+1],E=u[p][T],I=u[p+1][T],D=u[p+1][T+1];(p!==0||o>0)&&h.push(M,E,D),(p!==i-1||c<Math.PI)&&h.push(E,I,D)}this.setIndex(h),this.setAttribute("position",new rn(g,3)),this.setAttribute("normal",new rn(v,3)),this.setAttribute("uv",new rn(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.widthSegments,e.heightSegments,e.phiStart,e.phiLength,e.thetaStart,e.thetaLength)}};var Bo=class extends xr{constructor(e){super(),this.isMeshPhongMaterial=!0,this.type="MeshPhongMaterial",this.color=new tt(16777215),this.specular=new tt(1118481),this.shininess=30,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new tt(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=Pg,this.normalScale=new fe(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new _r,this.combine=Ld,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.specular.copy(e.specular),this.shininess=e.shininess,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}};var md=class extends xr{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=JM,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},gd=class extends xr{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function Yu(n,e,t){return!n||!t&&n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}function DP(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}var ds=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},vd=class extends ds{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:og,endingEnd:og}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case ag:s=e,a=2*t-i;break;case cg:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case ag:o=e,c=2*i-t;break;case cg:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,d=this._offsetNext,f=this._weightPrev,h=this._weightNext,g=(i-t)/(r-t),v=g*g,m=v*g,p=-f*m+2*f*v-f*g,T=(1+f)*m+(-1.5-2*f)*v+(-.5+f)*g+1,M=(-1-h)*m+(1.5+h)*v+.5*g,E=h*m-h*v;for(let I=0;I!==a;++I)s[I]=p*o[u+I]+T*o[l+I]+M*o[c+I]+E*o[d+I];return s}},yd=class extends ds{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),d=1-u;for(let f=0;f!==a;++f)s[f]=o[l+f]*d+o[c+f]*u;return s}},_d=class extends ds{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},zn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=Yu(t,this.TimeBufferType),this.values=Yu(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:Yu(e.times,Array),values:Yu(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new _d(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new yd(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new vd(this.times,this.values,this.getValueSize(),e)}setInterpolation(e){let t;switch(e){case qa:t=this.InterpolantFactoryMethodDiscrete;break;case td:t=this.InterpolantFactoryMethodLinear;break;case Zu:t=this.InterpolantFactoryMethodSmooth;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return console.warn("THREE.KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return qa;case this.InterpolantFactoryMethodLinear:return td;case this.InterpolantFactoryMethodSmooth:return Zu}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(console.error("THREE.KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(console.error("THREE.KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){console.error("THREE.KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){console.error("THREE.KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&DP(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){console.error("THREE.KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===Zu,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let d=a*i,f=d-i,h=d+i;for(let g=0;g!==i;++g){let v=t[d+g];if(v!==t[f+g]||v!==t[h+g]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let d=a*i,f=o*i;for(let h=0;h!==i;++h)t[f+h]=t[d+h]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};zn.prototype.TimeBufferType=Float32Array;zn.prototype.ValueBufferType=Float32Array;zn.prototype.DefaultInterpolation=td;var Mr=class extends zn{constructor(e,t,i){super(e,t,i)}};Mr.prototype.ValueTypeName="bool";Mr.prototype.ValueBufferType=Array;Mr.prototype.DefaultInterpolation=qa;Mr.prototype.InterpolantFactoryMethodLinear=void 0;Mr.prototype.InterpolantFactoryMethodSmooth=void 0;var xd=class extends zn{};xd.prototype.ValueTypeName="color";var Ed=class extends zn{};Ed.prototype.ValueTypeName="number";var Md=class extends ds{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)Hi.slerpFlat(s,0,o,l-a,o,l,c);return s}},hc=class extends zn{InterpolantFactoryMethodLinear(e){return new Md(this.times,this.values,this.getValueSize(),e)}};hc.prototype.ValueTypeName="quaternion";hc.prototype.InterpolantFactoryMethodSmooth=void 0;var Sr=class extends zn{constructor(e,t,i){super(e,t,i)}};Sr.prototype.ValueTypeName="string";Sr.prototype.ValueBufferType=Array;Sr.prototype.DefaultInterpolation=qa;Sr.prototype.InterpolantFactoryMethodLinear=void 0;Sr.prototype.InterpolantFactoryMethodSmooth=void 0;var Sd=class extends zn{};Sd.prototype.ValueTypeName="vector";var pc={enabled:!1,files:{},add:function(n,e){this.enabled!==!1&&(this.files[n]=e)},get:function(n){if(this.enabled!==!1)return this.files[n]},remove:function(n){delete this.files[n]},clear:function(){this.files={}}},wd=class{constructor(e,t,i){let r=this,s=!1,o=0,a=0,c,l=[];this.onStart=void 0,this.onLoad=e,this.onProgress=t,this.onError=i,this.itemStart=function(u){a++,s===!1&&r.onStart!==void 0&&r.onStart(u,o,a),s=!0},this.itemEnd=function(u){o++,r.onProgress!==void 0&&r.onProgress(u,o,a),o===a&&(s=!1,r.onLoad!==void 0&&r.onLoad())},this.itemError=function(u){r.onError!==void 0&&r.onError(u)},this.resolveURL=function(u){return c?c(u):u},this.setURLModifier=function(u){return c=u,this},this.addHandler=function(u,d){return l.push(u,d),this},this.removeHandler=function(u){let d=l.indexOf(u);return d!==-1&&l.splice(d,2),this},this.getHandler=function(u){for(let d=0,f=l.length;d<f;d+=2){let h=l[d],g=l[d+1];if(h.global&&(h.lastIndex=0),h.test(u))return g}return null}}},vS=new wd,Go=(()=>{class n{constructor(t){this.manager=t!==void 0?t:vS,this.crossOrigin="anonymous",this.withCredentials=!1,this.path="",this.resourcePath="",this.requestHeader={}}load(){}loadAsync(t,i){let r=this;return new Promise(function(s,o){r.load(t,s,i,o)})}parse(){}setCrossOrigin(t){return this.crossOrigin=t,this}setWithCredentials(t){return this.withCredentials=t,this}setPath(t){return this.path=t,this}setResourcePath(t){return this.resourcePath=t,this}setRequestHeader(t){return this.requestHeader=t,this}}return n.DEFAULT_MATERIAL_NAME="__DEFAULT",n})(),Ui={},pg=class extends Error{constructor(e,t){super(e),this.response=t}},mc=class extends Go{constructor(e){super(e)}load(e,t,i,r){e===void 0&&(e=""),this.path!==void 0&&(e=this.path+e),e=this.manager.resolveURL(e);let s=pc.get(e);if(s!==void 0)return this.manager.itemStart(e),setTimeout(()=>{t&&t(s),this.manager.itemEnd(e)},0),s;if(Ui[e]!==void 0){Ui[e].push({onLoad:t,onProgress:i,onError:r});return}Ui[e]=[],Ui[e].push({onLoad:t,onProgress:i,onError:r});let o=new Request(e,{headers:new Headers(this.requestHeader),credentials:this.withCredentials?"include":"same-origin"}),a=this.mimeType,c=this.responseType;fetch(o).then(l=>{if(l.status===200||l.status===0){if(l.status===0&&console.warn("THREE.FileLoader: HTTP Status 0 received."),typeof ReadableStream>"u"||l.body===void 0||l.body.getReader===void 0)return l;let u=Ui[e],d=l.body.getReader(),f=l.headers.get("X-File-Size")||l.headers.get("Content-Length"),h=f?parseInt(f):0,g=h!==0,v=0,m=new ReadableStream({start(p){T();function T(){d.read().then(({done:M,value:E})=>{if(M)p.close();else{v+=E.byteLength;let I=new ProgressEvent("progress",{lengthComputable:g,loaded:v,total:h});for(let D=0,C=u.length;D<C;D++){let N=u[D];N.onProgress&&N.onProgress(I)}p.enqueue(E),T()}},M=>{p.error(M)})}}});return new Response(m)}else throw new pg(`fetch for "${l.url}" responded with ${l.status}: ${l.statusText}`,l)}).then(l=>{switch(c){case"arraybuffer":return l.arrayBuffer();case"blob":return l.blob();case"document":return l.text().then(u=>new DOMParser().parseFromString(u,a));case"json":return l.json();default:if(a===void 0)return l.text();{let d=/charset="?([^;"\s]*)"?/i.exec(a),f=d&&d[1]?d[1].toLowerCase():void 0,h=new TextDecoder(f);return l.arrayBuffer().then(g=>h.decode(g))}}}).then(l=>{pc.add(e,l);let u=Ui[e];delete Ui[e];for(let d=0,f=u.length;d<f;d++){let h=u[d];h.onLoad&&h.onLoad(l)}}).catch(l=>{let u=Ui[e];if(u===void 0)throw this.manager.itemError(e),l;delete Ui[e];for(let d=0,f=u.length;d<f;d++){let h=u[d];h.onError&&h.onError(l)}this.manager.itemError(e)}).finally(()=>{this.manager.itemEnd(e)}),this.manager.itemStart(e)}setResponseType(e){return this.responseType=e,this}setMimeType(e){return this.mimeType=e,this}};var bd=class extends Go{constructor(e){super(e)}load(e,t,i,r){this.path!==void 0&&(e=this.path+e),e=this.manager.resolveURL(e);let s=this,o=pc.get(e);if(o!==void 0)return s.manager.itemStart(e),setTimeout(function(){t&&t(o),s.manager.itemEnd(e)},0),o;let a=No("img");function c(){u(),pc.add(e,this),t&&t(this),s.manager.itemEnd(e)}function l(d){u(),r&&r(d),s.manager.itemError(e),s.manager.itemEnd(e)}function u(){a.removeEventListener("load",c,!1),a.removeEventListener("error",l,!1)}return a.addEventListener("load",c,!1),a.addEventListener("error",l,!1),e.slice(0,5)!=="data:"&&this.crossOrigin!==void 0&&(a.crossOrigin=this.crossOrigin),s.manager.itemStart(e),a.src=e,a}};var fs=class extends Go{constructor(e){super(e)}load(e,t,i,r){let s=new $i,o=new bd(this.manager);return o.setCrossOrigin(this.crossOrigin),o.setPath(this.path),o.load(e,function(a){s.image=a,s.needsUpdate=!0,t!==void 0&&t(s)},i,r),s}},gc=class extends di{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new tt(e),this.intensity=t}dispose(){}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,this.groundColor!==void 0&&(t.object.groundColor=this.groundColor.getHex()),this.distance!==void 0&&(t.object.distance=this.distance),this.angle!==void 0&&(t.object.angle=this.angle),this.decay!==void 0&&(t.object.decay=this.decay),this.penumbra!==void 0&&(t.object.penumbra=this.penumbra),this.shadow!==void 0&&(t.object.shadow=this.shadow.toJSON()),this.target!==void 0&&(t.object.target=this.target.uuid),t}};var rg=new bt,_M=new L,xM=new L,mg=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new fe(512,512),this.map=null,this.mapPass=null,this.matrix=new bt,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new Fo,this._frameExtents=new fe(1,1),this._viewportCount=1,this._viewports=[new Tt(0,0,1,1)]}getViewportCount(){return this._viewportCount}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera,i=this.matrix;_M.setFromMatrixPosition(e.matrixWorld),t.position.copy(_M),xM.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(xM),t.updateMatrixWorld(),rg.multiplyMatrices(t.projectionMatrix,t.matrixWorldInverse),this._frustum.setFromProjectionMatrix(rg),i.set(.5,0,0,.5,0,.5,0,.5,0,0,.5,.5,0,0,0,1),i.multiply(rg)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.mapSize.copy(e.mapSize),this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return this.intensity!==1&&(e.intensity=this.intensity),this.bias!==0&&(e.bias=this.bias),this.normalBias!==0&&(e.normalBias=this.normalBias),this.radius!==1&&(e.radius=this.radius),(this.mapSize.x!==512||this.mapSize.y!==512)&&(e.mapSize=this.mapSize.toArray()),e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}};var vc=class extends tc{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},gg=class extends mg{constructor(){super(new vc(-5,5,5,-5,.5,500)),this.isDirectionalLightShadow=!0}},yc=class extends gc{constructor(e,t){super(e,t),this.isDirectionalLight=!0,this.type="DirectionalLight",this.position.copy(di.DEFAULT_UP),this.updateMatrix(),this.target=new di,this.shadow=new gg}dispose(){this.shadow.dispose()}copy(e){return super.copy(e),this.target=e.target.clone(),this.shadow=e.shadow.clone(),this}},_c=class extends gc{constructor(e,t){super(e,t),this.isAmbientLight=!0,this.type="AmbientLight"}};var Td=class extends Qt{constructor(e=[]){super(),this.isArrayCamera=!0,this.cameras=e,this.index=0}};var Bg="\\[\\]\\.:\\/",AP=new RegExp("["+Bg+"]","g"),Vg="[^"+Bg+"]",IP="[^"+Bg.replace("\\.","")+"]",RP=/((?:WC+[\/:])*)/.source.replace("WC",Vg),NP=/(WCOD+)?/.source.replace("WCOD",IP),PP=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",Vg),OP=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",Vg),LP=new RegExp("^"+RP+NP+PP+OP+"$"),FP=["material","materials","bones","map"],vg=class{constructor(e,t,i){let r=i||At.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},At=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(AP,"")}static parseTrackName(t){let i=LP.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);FP.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let c=o[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){console.warn("THREE.PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){console.error("THREE.PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){console.error("THREE.PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){console.error("THREE.PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){console.error("THREE.PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){console.error("THREE.PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;console.error("THREE.PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=vg,n})();At.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};At.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};At.prototype.GetterByBindingType=[At.prototype._getValue_direct,At.prototype._getValue_array,At.prototype._getValue_arrayElement,At.prototype._getValue_toArray];At.prototype.SetterByBindingTypeAndVersioning=[[At.prototype._setValue_direct,At.prototype._setValue_direct_setNeedsUpdate,At.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[At.prototype._setValue_array,At.prototype._setValue_array_setNeedsUpdate,At.prototype._setValue_array_setMatrixWorldNeedsUpdate],[At.prototype._setValue_arrayElement,At.prototype._setValue_arrayElement_setNeedsUpdate,At.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[At.prototype._setValue_fromArray,At.prototype._setValue_fromArray_setNeedsUpdate,At.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var f4=new Float32Array(1);var xc=class{constructor(){this.type="ShapePath",this.color=new tt,this.subPaths=[],this.currentPath=null}moveTo(e,t){return this.currentPath=new Uo,this.subPaths.push(this.currentPath),this.currentPath.moveTo(e,t),this}lineTo(e,t){return this.currentPath.lineTo(e,t),this}quadraticCurveTo(e,t,i,r){return this.currentPath.quadraticCurveTo(e,t,i,r),this}bezierCurveTo(e,t,i,r,s,o){return this.currentPath.bezierCurveTo(e,t,i,r,s,o),this}splineThru(e){return this.currentPath.splineThru(e),this}toShapes(e){function t(p){let T=[];for(let M=0,E=p.length;M<E;M++){let I=p[M],D=new ns;D.curves=I.curves,T.push(D)}return T}function i(p,T){let M=T.length,E=!1;for(let I=M-1,D=0;D<M;I=D++){let C=T[I],N=T[D],w=N.x-C.x,x=N.y-C.y;if(Math.abs(x)>Number.EPSILON){if(x<0&&(C=T[D],w=-w,N=T[I],x=-x),p.y<C.y||p.y>N.y)continue;if(p.y===C.y){if(p.x===C.x)return!0}else{let A=x*(p.x-C.x)-w*(p.y-C.y);if(A===0)return!0;if(A<0)continue;E=!E}}else{if(p.y!==C.y)continue;if(N.x<=p.x&&p.x<=C.x||C.x<=p.x&&p.x<=N.x)return!0}}return E}let r=is.isClockWise,s=this.subPaths;if(s.length===0)return[];let o,a,c,l=[];if(s.length===1)return a=s[0],c=new ns,c.curves=a.curves,l.push(c),l;let u=!r(s[0].getPoints());u=e?!u:u;let d=[],f=[],h=[],g=0,v;f[g]=void 0,h[g]=[];for(let p=0,T=s.length;p<T;p++)a=s[p],v=a.getPoints(),o=r(v),o=e?!o:o,o?(!u&&f[g]&&g++,f[g]={s:new ns,p:v},f[g].s.curves=a.curves,u&&g++,h[g]=[]):h[g].push({h:a,p:v[0]});if(!f[0])return t(s);if(f.length>1){let p=!1,T=0;for(let M=0,E=f.length;M<E;M++)d[M]=[];for(let M=0,E=f.length;M<E;M++){let I=h[M];for(let D=0;D<I.length;D++){let C=I[D],N=!0;for(let w=0;w<f.length;w++)i(C.p,f[w].p)&&(M!==w&&T++,N?(N=!1,d[w].push(C)):p=!0);N&&d[M].push(C)}}T>0&&p===!1&&(h=d)}let m;for(let p=0,T=f.length;p<T;p++){c=f[p].s,l.push(c),m=h[p];for(let M=0,E=m.length;M<E;M++)c.holes.push(m[M].h)}return l}};function zg(n,e,t,i){let r=kP(i);switch(t){case Tg:return n*e;case Dg:return n*e;case Ag:return n*e*2;case Ig:return n*e/r.components*r.byteLength;case Hd:return n*e/r.components*r.byteLength;case Rg:return n*e*2/r.components*r.byteLength;case Gd:return n*e*2/r.components*r.byteLength;case Cg:return n*e*3/r.components*r.byteLength;case Hn:return n*e*4/r.components*r.byteLength;case jd:return n*e*4/r.components*r.byteLength;case Sc:case wc:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case bc:case Tc:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case $d:case Xd:return Math.max(n,16)*Math.max(e,8)/4;case Wd:case qd:return Math.max(n,8)*Math.max(e,8)/2;case Yd:case Zd:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Kd:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Jd:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Qd:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case ef:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case tf:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case nf:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case rf:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case sf:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case of:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case af:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case cf:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case lf:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case uf:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case df:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case ff:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Cc:case hf:case pf:return Math.ceil(n/4)*Math.ceil(e/4)*16;case Ng:case mf:return Math.ceil(n/4)*Math.ceil(e/4)*8;case gf:case vf:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function kP(n){switch(n){case mi:case Sg:return{byteLength:1,components:1};case Vo:case wg:case zo:return{byteLength:2,components:1};case Vd:case zd:return{byteLength:2,components:4};case br:case Bd:case gi:return{byteLength:4,components:1};case bg:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:Cd}}));typeof window<"u"&&(window.__THREE__?console.warn("WARNING: Multiple instances of Three.js being imported."):window.__THREE__=Cd);function zS(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function UP(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,d=l.byteLength,f=n.createBuffer();n.bindBuffer(c,f),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:f,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,c,l){let u=c.array,d=c.updateRanges;if(n.bindBuffer(l,a),d.length===0)n.bufferSubData(l,0,u);else{d.sort((h,g)=>h.start-g.start);let f=0;for(let h=1;h<d.length;h++){let g=d[f],v=d[h];v.start<=g.start+g.count+1?g.count=Math.max(g.count,v.start+v.count-g.start):(++f,d[f]=v)}d.length=f+1;for(let h=0,g=d.length;h<g;h++){let v=d[h];n.bufferSubData(l,v.start*u.BYTES_PER_ELEMENT,u,v.start,v.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var BP=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,VP=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,zP=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,HP=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,GP=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,jP=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,WP=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,$P=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,qP=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec3 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 ).rgb;
	}
#endif`,XP=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,YP=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,ZP=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,KP=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,JP=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,QP=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,eO=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,tO=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,nO=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,iO=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,rO=`#if defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#elif defined( USE_COLOR )
	diffuseColor.rgb *= vColor;
#endif`,sO=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR )
	varying vec3 vColor;
#endif`,oO=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec3 vColor;
#endif`,aO=`#if defined( USE_COLOR_ALPHA )
	vColor = vec4( 1.0 );
#elif defined( USE_COLOR ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec3( 1.0 );
#endif
#ifdef USE_COLOR
	vColor *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.xyz *= instanceColor.xyz;
#endif
#ifdef USE_BATCHING_COLOR
	vec3 batchingColor = getBatchingColor( getIndirectIndex( gl_DrawID ) );
	vColor.xyz *= batchingColor.xyz;
#endif`,cO=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
mat3 transposeMat3( const in mat3 m ) {
	mat3 tmp;
	tmp[ 0 ] = vec3( m[ 0 ].x, m[ 1 ].x, m[ 2 ].x );
	tmp[ 1 ] = vec3( m[ 0 ].y, m[ 1 ].y, m[ 2 ].y );
	tmp[ 2 ] = vec3( m[ 0 ].z, m[ 1 ].z, m[ 2 ].z );
	return tmp;
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,lO=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,uO=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,dO=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,fO=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,hO=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,pO=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,mO="gl_FragColor = linearToOutputTexel( gl_FragColor );",gO=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,vO=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * vec3( flipEnvMap * reflectVec.x, reflectVec.yz ) );
	#else
		vec4 envColor = vec4( 0.0 );
	#endif
	#ifdef ENVMAP_BLENDING_MULTIPLY
		outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
	#elif defined( ENVMAP_BLENDING_MIX )
		outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
	#elif defined( ENVMAP_BLENDING_ADD )
		outgoingLight += envColor.xyz * specularStrength * reflectivity;
	#endif
#endif`,yO=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform float flipEnvMap;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
	
#endif`,_O=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,xO=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,EO=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,MO=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,SO=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,wO=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,bO=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,TO=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,CO=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,DO=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,AO=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,IO=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif`,RO=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, roughness * roughness) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,NO=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,PO=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,OO=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,LO=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,FO=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb * ( 1.0 - metalnessFactor );
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = mix( min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = mix( vec3( 0.04 ), diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.07, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,kO=`struct PhysicalMaterial {
	vec3 diffuseColor;
	float roughness;
	vec3 specularColor;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		float v = 0.5 / ( gv + gl );
		return saturate(v);
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColor;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transposeMat3( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float a = roughness < 0.25 ? -339.2 * r2 + 161.4 * roughness - 25.9 : -8.48 * r2 + 14.3 * roughness - 9.95;
	float b = roughness < 0.25 ? 44.0 * r2 - 23.7 * roughness + 3.26 : 1.97 * r2 - 3.27 * roughness + 0.72;
	float DG = exp( a * dotNV + b ) + ( roughness < 0.25 ? 0.0 : 0.1 * ( roughness - 0.25 ) );
	return saturate( DG * RECIPROCAL_PI );
}
vec2 DFGApprox( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	const vec4 c0 = vec4( - 1, - 0.0275, - 0.572, 0.022 );
	const vec4 c1 = vec4( 1, 0.0425, 1.04, - 0.04 );
	vec4 r = roughness * c0 + c1;
	float a004 = min( r.x * r.x, exp2( - 9.28 * dotNV ) ) * r.x + r.y;
	vec2 fab = vec2( - 1.04, 1.04 ) * a004 + r.zw;
	return fab;
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	vec2 fab = DFGApprox( normal, viewDir, roughness );
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	vec2 fab = DFGApprox( normal, viewDir, roughness );
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColor * t2.x + ( vec3( 1.0 ) - material.specularColor ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseColor * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
	#endif
	vec3 singleScattering = vec3( 0.0 );
	vec3 multiScattering = vec3( 0.0 );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnel, material.roughness, singleScattering, multiScattering );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScattering, multiScattering );
	#endif
	vec3 totalScattering = singleScattering + multiScattering;
	vec3 diffuse = material.diffuseColor * ( 1.0 - max( max( totalScattering.r, totalScattering.g ), totalScattering.b ) );
	reflectedLight.indirectSpecular += radiance * singleScattering;
	reflectedLight.indirectSpecular += multiScattering * cosineWeightedIrradiance;
	reflectedLight.indirectDiffuse += diffuse * cosineWeightedIrradiance;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,UO=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnel = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,BO=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD ) && defined( ENVMAP_TYPE_CUBE_UV )
		iblIrradiance += getIBLIrradiance( geometryNormal );
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,VO=`#if defined( RE_IndirectDiffuse )
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,zO=`#if defined( USE_LOGDEPTHBUF )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,HO=`#if defined( USE_LOGDEPTHBUF )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,GO=`#ifdef USE_LOGDEPTHBUF
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,jO=`#ifdef USE_LOGDEPTHBUF
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,WO=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,$O=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,qO=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,XO=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,YO=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,ZO=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,KO=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,JO=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,QO=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,eL=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,tL=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,nL=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,iL=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,rL=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,sL=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,oL=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,aL=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,cL=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,lL=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,uL=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,dL=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,fL=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,hL=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	return depth * ( near - far ) - near;
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	return ( near * far ) / ( ( far - near ) * depth - far );
}`,pL=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,mL=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,gL=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,vL=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,yL=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,_L=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,xL=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform sampler2D pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	float texture2DCompare( sampler2D depths, vec2 uv, float compare ) {
		return step( compare, unpackRGBAToDepth( texture2D( depths, uv ) ) );
	}
	vec2 texture2DDistribution( sampler2D shadow, vec2 uv ) {
		return unpackRGBATo2Half( texture2D( shadow, uv ) );
	}
	float VSMShadow (sampler2D shadow, vec2 uv, float compare ){
		float occlusion = 1.0;
		vec2 distribution = texture2DDistribution( shadow, uv );
		float hard_shadow = step( compare , distribution.x );
		if (hard_shadow != 1.0 ) {
			float distance = compare - distribution.x ;
			float variance = max( 0.00000, distribution.y * distribution.y );
			float softness_probability = variance / (variance + distance * distance );			softness_probability = clamp( ( softness_probability - 0.3 ) / ( 0.95 - 0.3 ), 0.0, 1.0 );			occlusion = clamp( max( hard_shadow, softness_probability ), 0.0, 1.0 );
		}
		return occlusion;
	}
	float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
		float shadow = 1.0;
		shadowCoord.xyz /= shadowCoord.w;
		shadowCoord.z += shadowBias;
		bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
		bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
		if ( frustumTest ) {
		#if defined( SHADOWMAP_TYPE_PCF )
			vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
			float dx0 = - texelSize.x * shadowRadius;
			float dy0 = - texelSize.y * shadowRadius;
			float dx1 = + texelSize.x * shadowRadius;
			float dy1 = + texelSize.y * shadowRadius;
			float dx2 = dx0 / 2.0;
			float dy2 = dy0 / 2.0;
			float dx3 = dx1 / 2.0;
			float dy3 = dy1 / 2.0;
			shadow = (
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx0, dy0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx1, dy0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx2, dy2 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy2 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx3, dy2 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx0, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx2, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy, shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx3, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx1, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx2, dy3 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy3 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx3, dy3 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx0, dy1 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy1 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx1, dy1 ), shadowCoord.z )
			) * ( 1.0 / 17.0 );
		#elif defined( SHADOWMAP_TYPE_PCF_SOFT )
			vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
			float dx = texelSize.x;
			float dy = texelSize.y;
			vec2 uv = shadowCoord.xy;
			vec2 f = fract( uv * shadowMapSize + 0.5 );
			uv -= f * texelSize;
			shadow = (
				texture2DCompare( shadowMap, uv, shadowCoord.z ) +
				texture2DCompare( shadowMap, uv + vec2( dx, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, uv + vec2( 0.0, dy ), shadowCoord.z ) +
				texture2DCompare( shadowMap, uv + texelSize, shadowCoord.z ) +
				mix( texture2DCompare( shadowMap, uv + vec2( -dx, 0.0 ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, 0.0 ), shadowCoord.z ),
					 f.x ) +
				mix( texture2DCompare( shadowMap, uv + vec2( -dx, dy ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, dy ), shadowCoord.z ),
					 f.x ) +
				mix( texture2DCompare( shadowMap, uv + vec2( 0.0, -dy ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( 0.0, 2.0 * dy ), shadowCoord.z ),
					 f.y ) +
				mix( texture2DCompare( shadowMap, uv + vec2( dx, -dy ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( dx, 2.0 * dy ), shadowCoord.z ),
					 f.y ) +
				mix( mix( texture2DCompare( shadowMap, uv + vec2( -dx, -dy ), shadowCoord.z ),
						  texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, -dy ), shadowCoord.z ),
						  f.x ),
					 mix( texture2DCompare( shadowMap, uv + vec2( -dx, 2.0 * dy ), shadowCoord.z ),
						  texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, 2.0 * dy ), shadowCoord.z ),
						  f.x ),
					 f.y )
			) * ( 1.0 / 9.0 );
		#elif defined( SHADOWMAP_TYPE_VSM )
			shadow = VSMShadow( shadowMap, shadowCoord.xy, shadowCoord.z );
		#else
			shadow = texture2DCompare( shadowMap, shadowCoord.xy, shadowCoord.z );
		#endif
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	vec2 cubeToUV( vec3 v, float texelSizeY ) {
		vec3 absV = abs( v );
		float scaleToCube = 1.0 / max( absV.x, max( absV.y, absV.z ) );
		absV *= scaleToCube;
		v *= scaleToCube * ( 1.0 - 2.0 * texelSizeY );
		vec2 planar = v.xy;
		float almostATexel = 1.5 * texelSizeY;
		float almostOne = 1.0 - almostATexel;
		if ( absV.z >= almostOne ) {
			if ( v.z > 0.0 )
				planar.x = 4.0 - v.x;
		} else if ( absV.x >= almostOne ) {
			float signX = sign( v.x );
			planar.x = v.z * signX + 2.0 * signX;
		} else if ( absV.y >= almostOne ) {
			float signY = sign( v.y );
			planar.x = v.x + 2.0 * signY + 2.0;
			planar.y = v.z * signY - 2.0;
		}
		return vec2( 0.125, 0.25 ) * planar + vec2( 0.375, 0.75 );
	}
	float getPointShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		
		float lightToPositionLength = length( lightToPosition );
		if ( lightToPositionLength - shadowCameraFar <= 0.0 && lightToPositionLength - shadowCameraNear >= 0.0 ) {
			float dp = ( lightToPositionLength - shadowCameraNear ) / ( shadowCameraFar - shadowCameraNear );			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			vec2 texelSize = vec2( 1.0 ) / ( shadowMapSize * vec2( 4.0, 2.0 ) );
			#if defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_PCF_SOFT ) || defined( SHADOWMAP_TYPE_VSM )
				vec2 offset = vec2( - 1, 1 ) * shadowRadius * texelSize.y;
				shadow = (
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xyy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yyy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xyx, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yyx, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xxy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yxy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xxx, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yxx, texelSize.y ), dp )
				) * ( 1.0 / 9.0 );
			#else
				shadow = texture2DCompare( shadowMap, cubeToUV( bd3D, texelSize.y ), dp );
			#endif
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
#endif`,EL=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,ML=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,SL=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,wL=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,bL=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,TL=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,CL=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,DL=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,AL=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,IL=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,RL=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,NL=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseColor, material.specularColor, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,PL=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		#else
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,OL=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,LL=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,FL=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,kL=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,UL=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,BL=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,VL=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,zL=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float flipEnvMap;
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vec3( flipEnvMap * vWorldDirection.x, vWorldDirection.yz ) );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,HL=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,GL=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,jL=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,WL=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	float fragCoordZ = 0.5 * vHighPrecisionZW[0] / vHighPrecisionZW[1] + 0.5;
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,$L=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,qL=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = packDepthToRGBA( dist );
}`,XL=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,YL=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,ZL=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,KL=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,JL=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,QL=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,eF=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,tF=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,nF=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,iF=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,rF=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,sF=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <packing>
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( packNormalToRGB( normal ), diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,oF=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,aF=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,cF=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,lF=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
		float sheenEnergyComp = 1.0 - 0.157 * max3( material.sheenColor );
		outgoingLight = outgoingLight * sheenEnergyComp + sheenSpecularDirect + sheenSpecularIndirect;
	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,uF=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,dF=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,fF=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,hF=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,pF=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,mF=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <packing>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,gF=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,vF=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,Ze={alphahash_fragment:BP,alphahash_pars_fragment:VP,alphamap_fragment:zP,alphamap_pars_fragment:HP,alphatest_fragment:GP,alphatest_pars_fragment:jP,aomap_fragment:WP,aomap_pars_fragment:$P,batching_pars_vertex:qP,batching_vertex:XP,begin_vertex:YP,beginnormal_vertex:ZP,bsdfs:KP,iridescence_fragment:JP,bumpmap_pars_fragment:QP,clipping_planes_fragment:eO,clipping_planes_pars_fragment:tO,clipping_planes_pars_vertex:nO,clipping_planes_vertex:iO,color_fragment:rO,color_pars_fragment:sO,color_pars_vertex:oO,color_vertex:aO,common:cO,cube_uv_reflection_fragment:lO,defaultnormal_vertex:uO,displacementmap_pars_vertex:dO,displacementmap_vertex:fO,emissivemap_fragment:hO,emissivemap_pars_fragment:pO,colorspace_fragment:mO,colorspace_pars_fragment:gO,envmap_fragment:vO,envmap_common_pars_fragment:yO,envmap_pars_fragment:_O,envmap_pars_vertex:xO,envmap_physical_pars_fragment:RO,envmap_vertex:EO,fog_vertex:MO,fog_pars_vertex:SO,fog_fragment:wO,fog_pars_fragment:bO,gradientmap_pars_fragment:TO,lightmap_pars_fragment:CO,lights_lambert_fragment:DO,lights_lambert_pars_fragment:AO,lights_pars_begin:IO,lights_toon_fragment:NO,lights_toon_pars_fragment:PO,lights_phong_fragment:OO,lights_phong_pars_fragment:LO,lights_physical_fragment:FO,lights_physical_pars_fragment:kO,lights_fragment_begin:UO,lights_fragment_maps:BO,lights_fragment_end:VO,logdepthbuf_fragment:zO,logdepthbuf_pars_fragment:HO,logdepthbuf_pars_vertex:GO,logdepthbuf_vertex:jO,map_fragment:WO,map_pars_fragment:$O,map_particle_fragment:qO,map_particle_pars_fragment:XO,metalnessmap_fragment:YO,metalnessmap_pars_fragment:ZO,morphinstance_vertex:KO,morphcolor_vertex:JO,morphnormal_vertex:QO,morphtarget_pars_vertex:eL,morphtarget_vertex:tL,normal_fragment_begin:nL,normal_fragment_maps:iL,normal_pars_fragment:rL,normal_pars_vertex:sL,normal_vertex:oL,normalmap_pars_fragment:aL,clearcoat_normal_fragment_begin:cL,clearcoat_normal_fragment_maps:lL,clearcoat_pars_fragment:uL,iridescence_pars_fragment:dL,opaque_fragment:fL,packing:hL,premultiplied_alpha_fragment:pL,project_vertex:mL,dithering_fragment:gL,dithering_pars_fragment:vL,roughnessmap_fragment:yL,roughnessmap_pars_fragment:_L,shadowmap_pars_fragment:xL,shadowmap_pars_vertex:EL,shadowmap_vertex:ML,shadowmask_pars_fragment:SL,skinbase_vertex:wL,skinning_pars_vertex:bL,skinning_vertex:TL,skinnormal_vertex:CL,specularmap_fragment:DL,specularmap_pars_fragment:AL,tonemapping_fragment:IL,tonemapping_pars_fragment:RL,transmission_fragment:NL,transmission_pars_fragment:PL,uv_pars_fragment:OL,uv_pars_vertex:LL,uv_vertex:FL,worldpos_vertex:kL,background_vert:UL,background_frag:BL,backgroundCube_vert:VL,backgroundCube_frag:zL,cube_vert:HL,cube_frag:GL,depth_vert:jL,depth_frag:WL,distanceRGBA_vert:$L,distanceRGBA_frag:qL,equirect_vert:XL,equirect_frag:YL,linedashed_vert:ZL,linedashed_frag:KL,meshbasic_vert:JL,meshbasic_frag:QL,meshlambert_vert:eF,meshlambert_frag:tF,meshmatcap_vert:nF,meshmatcap_frag:iF,meshnormal_vert:rF,meshnormal_frag:sF,meshphong_vert:oF,meshphong_frag:aF,meshphysical_vert:cF,meshphysical_frag:lF,meshtoon_vert:uF,meshtoon_frag:dF,points_vert:fF,points_frag:hF,shadow_vert:pF,shadow_frag:mF,sprite_vert:gF,sprite_frag:vF},ce={common:{diffuse:{value:new tt(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new je},alphaMap:{value:null},alphaMapTransform:{value:new je},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new je}},envmap:{envMap:{value:null},envMapRotation:{value:new je},flipEnvMap:{value:-1},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new je}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new je}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new je},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new je},normalScale:{value:new fe(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new je},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new je}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new je}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new je}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new tt(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMap:{value:[]},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotShadowMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMap:{value:[]},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null}},points:{diffuse:{value:new tt(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new je},alphaTest:{value:0},uvTransform:{value:new je}},sprite:{diffuse:{value:new tt(16777215)},opacity:{value:1},center:{value:new fe(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new je},alphaMap:{value:null},alphaMapTransform:{value:new je},alphaTest:{value:0}}},vi={basic:{uniforms:en([ce.common,ce.specularmap,ce.envmap,ce.aomap,ce.lightmap,ce.fog]),vertexShader:Ze.meshbasic_vert,fragmentShader:Ze.meshbasic_frag},lambert:{uniforms:en([ce.common,ce.specularmap,ce.envmap,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.fog,ce.lights,{emissive:{value:new tt(0)}}]),vertexShader:Ze.meshlambert_vert,fragmentShader:Ze.meshlambert_frag},phong:{uniforms:en([ce.common,ce.specularmap,ce.envmap,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.fog,ce.lights,{emissive:{value:new tt(0)},specular:{value:new tt(1118481)},shininess:{value:30}}]),vertexShader:Ze.meshphong_vert,fragmentShader:Ze.meshphong_frag},standard:{uniforms:en([ce.common,ce.envmap,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.roughnessmap,ce.metalnessmap,ce.fog,ce.lights,{emissive:{value:new tt(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:Ze.meshphysical_vert,fragmentShader:Ze.meshphysical_frag},toon:{uniforms:en([ce.common,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.gradientmap,ce.fog,ce.lights,{emissive:{value:new tt(0)}}]),vertexShader:Ze.meshtoon_vert,fragmentShader:Ze.meshtoon_frag},matcap:{uniforms:en([ce.common,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.fog,{matcap:{value:null}}]),vertexShader:Ze.meshmatcap_vert,fragmentShader:Ze.meshmatcap_frag},points:{uniforms:en([ce.points,ce.fog]),vertexShader:Ze.points_vert,fragmentShader:Ze.points_frag},dashed:{uniforms:en([ce.common,ce.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:Ze.linedashed_vert,fragmentShader:Ze.linedashed_frag},depth:{uniforms:en([ce.common,ce.displacementmap]),vertexShader:Ze.depth_vert,fragmentShader:Ze.depth_frag},normal:{uniforms:en([ce.common,ce.bumpmap,ce.normalmap,ce.displacementmap,{opacity:{value:1}}]),vertexShader:Ze.meshnormal_vert,fragmentShader:Ze.meshnormal_frag},sprite:{uniforms:en([ce.sprite,ce.fog]),vertexShader:Ze.sprite_vert,fragmentShader:Ze.sprite_frag},background:{uniforms:{uvTransform:{value:new je},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:Ze.background_vert,fragmentShader:Ze.background_frag},backgroundCube:{uniforms:{envMap:{value:null},flipEnvMap:{value:-1},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new je}},vertexShader:Ze.backgroundCube_vert,fragmentShader:Ze.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:Ze.cube_vert,fragmentShader:Ze.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:Ze.equirect_vert,fragmentShader:Ze.equirect_frag},distanceRGBA:{uniforms:en([ce.common,ce.displacementmap,{referencePosition:{value:new L},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:Ze.distanceRGBA_vert,fragmentShader:Ze.distanceRGBA_frag},shadow:{uniforms:en([ce.lights,ce.fog,{color:{value:new tt(0)},opacity:{value:1}}]),vertexShader:Ze.shadow_vert,fragmentShader:Ze.shadow_frag}};vi.physical={uniforms:en([vi.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new je},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new je},clearcoatNormalScale:{value:new fe(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new je},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new je},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new je},sheen:{value:0},sheenColor:{value:new tt(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new je},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new je},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new je},transmissionSamplerSize:{value:new fe},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new je},attenuationDistance:{value:0},attenuationColor:{value:new tt(0)},specularColor:{value:new tt(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new je},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new je},anisotropyVector:{value:new fe},anisotropyMap:{value:null},anisotropyMapTransform:{value:new je}}]),vertexShader:Ze.meshphysical_vert,fragmentShader:Ze.meshphysical_frag};var _f={r:0,b:0,g:0},ys=new _r,yF=new bt;function _F(n,e,t,i,r,s,o){let a=new tt(0),c=s===!0?0:1,l,u,d=null,f=0,h=null;function g(M){let E=M.isScene===!0?M.background:null;return E&&E.isTexture&&(E=(M.backgroundBlurriness>0?t:e).get(E)),E}function v(M){let E=!1,I=g(M);I===null?p(a,c):I&&I.isColor&&(p(I,1),E=!0);let D=n.xr.getEnvironmentBlendMode();D==="additive"?i.buffers.color.setClear(0,0,0,1,o):D==="alpha-blend"&&i.buffers.color.setClear(0,0,0,0,o),(n.autoClear||E)&&(i.buffers.depth.setTest(!0),i.buffers.depth.setMask(!0),i.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function m(M,E){let I=g(E);I&&(I.isCubeTexture||I.mapping===Ec)?(u===void 0&&(u=new Gt(new Oo(1,1,1),new ei({name:"BackgroundCubeMaterial",uniforms:vs(vi.backgroundCube.uniforms),vertexShader:vi.backgroundCube.vertexShader,fragmentShader:vi.backgroundCube.fragmentShader,side:sn,depthTest:!1,depthWrite:!1,fog:!1})),u.geometry.deleteAttribute("normal"),u.geometry.deleteAttribute("uv"),u.onBeforeRender=function(D,C,N){this.matrixWorld.copyPosition(N.matrixWorld)},Object.defineProperty(u.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),r.update(u)),ys.copy(E.backgroundRotation),ys.x*=-1,ys.y*=-1,ys.z*=-1,I.isCubeTexture&&I.isRenderTargetTexture===!1&&(ys.y*=-1,ys.z*=-1),u.material.uniforms.envMap.value=I,u.material.uniforms.flipEnvMap.value=I.isCubeTexture&&I.isRenderTargetTexture===!1?-1:1,u.material.uniforms.backgroundBlurriness.value=E.backgroundBlurriness,u.material.uniforms.backgroundIntensity.value=E.backgroundIntensity,u.material.uniforms.backgroundRotation.value.setFromMatrix4(yF.makeRotationFromEuler(ys)),u.material.toneMapped=ct.getTransfer(I.colorSpace)!==_t,(d!==I||f!==I.version||h!==n.toneMapping)&&(u.material.needsUpdate=!0,d=I,f=I.version,h=n.toneMapping),u.layers.enableAll(),M.unshift(u,u.geometry,u.material,0,0,null)):I&&I.isTexture&&(l===void 0&&(l=new Gt(new ls(2,2),new ei({name:"BackgroundMaterial",uniforms:vs(vi.background.uniforms),vertexShader:vi.background.vertexShader,fragmentShader:vi.background.fragmentShader,side:Vi,depthTest:!1,depthWrite:!1,fog:!1})),l.geometry.deleteAttribute("normal"),Object.defineProperty(l.material,"map",{get:function(){return this.uniforms.t2D.value}}),r.update(l)),l.material.uniforms.t2D.value=I,l.material.uniforms.backgroundIntensity.value=E.backgroundIntensity,l.material.toneMapped=ct.getTransfer(I.colorSpace)!==_t,I.matrixAutoUpdate===!0&&I.updateMatrix(),l.material.uniforms.uvTransform.value.copy(I.matrix),(d!==I||f!==I.version||h!==n.toneMapping)&&(l.material.needsUpdate=!0,d=I,f=I.version,h=n.toneMapping),l.layers.enableAll(),M.unshift(l,l.geometry,l.material,0,0,null))}function p(M,E){M.getRGB(_f,kg(n)),i.buffers.color.setClear(_f.r,_f.g,_f.b,E,o)}function T(){u!==void 0&&(u.geometry.dispose(),u.material.dispose(),u=void 0),l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0)}return{getClearColor:function(){return a},setClearColor:function(M,E=1){a.set(M),c=E,p(a,c)},getClearAlpha:function(){return c},setClearAlpha:function(M){c=M,p(a,c)},render:v,addToRenderList:m,dispose:T}}function xF(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=f(null),s=r,o=!1;function a(x,A,$,B,q){let X=!1,H=d(B,$,A);s!==H&&(s=H,l(s.object)),X=h(x,B,$,q),X&&g(x,B,$,q),q!==null&&e.update(q,n.ELEMENT_ARRAY_BUFFER),(X||o)&&(o=!1,E(x,A,$,B),q!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(q).buffer))}function c(){return n.createVertexArray()}function l(x){return n.bindVertexArray(x)}function u(x){return n.deleteVertexArray(x)}function d(x,A,$){let B=$.wireframe===!0,q=i[x.id];q===void 0&&(q={},i[x.id]=q);let X=q[A.id];X===void 0&&(X={},q[A.id]=X);let H=X[B];return H===void 0&&(H=f(c()),X[B]=H),H}function f(x){let A=[],$=[],B=[];for(let q=0;q<t;q++)A[q]=0,$[q]=0,B[q]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:A,enabledAttributes:$,attributeDivisors:B,object:x,attributes:{},index:null}}function h(x,A,$,B){let q=s.attributes,X=A.attributes,H=0,Z=$.getAttributes();for(let V in Z)if(Z[V].location>=0){let ge=q[V],Se=X[V];if(Se===void 0&&(V==="instanceMatrix"&&x.instanceMatrix&&(Se=x.instanceMatrix),V==="instanceColor"&&x.instanceColor&&(Se=x.instanceColor)),ge===void 0||ge.attribute!==Se||Se&&ge.data!==Se.data)return!0;H++}return s.attributesNum!==H||s.index!==B}function g(x,A,$,B){let q={},X=A.attributes,H=0,Z=$.getAttributes();for(let V in Z)if(Z[V].location>=0){let ge=X[V];ge===void 0&&(V==="instanceMatrix"&&x.instanceMatrix&&(ge=x.instanceMatrix),V==="instanceColor"&&x.instanceColor&&(ge=x.instanceColor));let Se={};Se.attribute=ge,ge&&ge.data&&(Se.data=ge.data),q[V]=Se,H++}s.attributes=q,s.attributesNum=H,s.index=B}function v(){let x=s.newAttributes;for(let A=0,$=x.length;A<$;A++)x[A]=0}function m(x){p(x,0)}function p(x,A){let $=s.newAttributes,B=s.enabledAttributes,q=s.attributeDivisors;$[x]=1,B[x]===0&&(n.enableVertexAttribArray(x),B[x]=1),q[x]!==A&&(n.vertexAttribDivisor(x,A),q[x]=A)}function T(){let x=s.newAttributes,A=s.enabledAttributes;for(let $=0,B=A.length;$<B;$++)A[$]!==x[$]&&(n.disableVertexAttribArray($),A[$]=0)}function M(x,A,$,B,q,X,H){H===!0?n.vertexAttribIPointer(x,A,$,q,X):n.vertexAttribPointer(x,A,$,B,q,X)}function E(x,A,$,B){v();let q=B.attributes,X=$.getAttributes(),H=A.defaultAttributeValues;for(let Z in X){let V=X[Z];if(V.location>=0){let ae=q[Z];if(ae===void 0&&(Z==="instanceMatrix"&&x.instanceMatrix&&(ae=x.instanceMatrix),Z==="instanceColor"&&x.instanceColor&&(ae=x.instanceColor)),ae!==void 0){let ge=ae.normalized,Se=ae.itemSize,Ge=e.get(ae);if(Ge===void 0)continue;let dt=Ge.buffer,W=Ge.type,se=Ge.bytesPerElement,we=W===n.INT||W===n.UNSIGNED_INT||ae.gpuType===Bd;if(ae.isInterleavedBufferAttribute){let oe=ae.data,Ie=oe.stride,ot=ae.offset;if(oe.isInstancedInterleavedBuffer){for(let De=0;De<V.locationSize;De++)p(V.location+De,oe.meshPerAttribute);x.isInstancedMesh!==!0&&B._maxInstanceCount===void 0&&(B._maxInstanceCount=oe.meshPerAttribute*oe.count)}else for(let De=0;De<V.locationSize;De++)m(V.location+De);n.bindBuffer(n.ARRAY_BUFFER,dt);for(let De=0;De<V.locationSize;De++)M(V.location+De,Se/V.locationSize,W,ge,Ie*se,(ot+Se/V.locationSize*De)*se,we)}else{if(ae.isInstancedBufferAttribute){for(let oe=0;oe<V.locationSize;oe++)p(V.location+oe,ae.meshPerAttribute);x.isInstancedMesh!==!0&&B._maxInstanceCount===void 0&&(B._maxInstanceCount=ae.meshPerAttribute*ae.count)}else for(let oe=0;oe<V.locationSize;oe++)m(V.location+oe);n.bindBuffer(n.ARRAY_BUFFER,dt);for(let oe=0;oe<V.locationSize;oe++)M(V.location+oe,Se/V.locationSize,W,ge,Se*se,Se/V.locationSize*oe*se,we)}}else if(H!==void 0){let ge=H[Z];if(ge!==void 0)switch(ge.length){case 2:n.vertexAttrib2fv(V.location,ge);break;case 3:n.vertexAttrib3fv(V.location,ge);break;case 4:n.vertexAttrib4fv(V.location,ge);break;default:n.vertexAttrib1fv(V.location,ge)}}}}T()}function I(){N();for(let x in i){let A=i[x];for(let $ in A){let B=A[$];for(let q in B)u(B[q].object),delete B[q];delete A[$]}delete i[x]}}function D(x){if(i[x.id]===void 0)return;let A=i[x.id];for(let $ in A){let B=A[$];for(let q in B)u(B[q].object),delete B[q];delete A[$]}delete i[x.id]}function C(x){for(let A in i){let $=i[A];if($[x.id]===void 0)continue;let B=$[x.id];for(let q in B)u(B[q].object),delete B[q];delete $[x.id]}}function N(){w(),o=!0,s!==r&&(s=r,l(s.object))}function w(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:N,resetDefaultState:w,dispose:I,releaseStatesOfGeometry:D,releaseStatesOfProgram:C,initAttributes:v,enableAttribute:m,disableUnusedAttributes:T}}function EF(n,e,t){let i;function r(l){i=l}function s(l,u){n.drawArrays(i,l,u),t.update(u,i,1)}function o(l,u,d){d!==0&&(n.drawArraysInstanced(i,l,u,d),t.update(u,i,d))}function a(l,u,d){if(d===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,l,0,u,0,d);let h=0;for(let g=0;g<d;g++)h+=u[g];t.update(h,i,1)}function c(l,u,d,f){if(d===0)return;let h=e.get("WEBGL_multi_draw");if(h===null)for(let g=0;g<l.length;g++)o(l[g],u[g],f[g]);else{h.multiDrawArraysInstancedWEBGL(i,l,0,u,0,f,0,d);let g=0;for(let v=0;v<d;v++)g+=u[v]*f[v];t.update(g,i,1)}}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a,this.renderMultiDrawInstances=c}function MF(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let C=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(C.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(C){return!(C!==Hn&&i.convert(C)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(C){let N=C===zo&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(C!==mi&&i.convert(C)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&C!==gi&&!N)}function c(C){if(C==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";C="mediump"}return C==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(console.warn("THREE.WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let d=t.logarithmicDepthBuffer===!0,f=t.reverseDepthBuffer===!0&&e.has("EXT_clip_control"),h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),v=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),T=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),M=n.getParameter(n.MAX_VARYING_VECTORS),E=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),I=g>0,D=n.getParameter(n.MAX_SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:d,reverseDepthBuffer:f,maxTextures:h,maxVertexTextures:g,maxTextureSize:v,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:T,maxVaryings:M,maxFragmentUniforms:E,vertexTextures:I,maxSamples:D}}function SF(n){let e=this,t=null,i=0,r=!1,s=!1,o=new ci,a=new je,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(d,f){let h=d.length!==0||f||i!==0||r;return r=f,i=d.length,h},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,f){t=u(d,f,0)},this.setState=function(d,f,h){let g=d.clippingPlanes,v=d.clipIntersection,m=d.clipShadows,p=n.get(d);if(!r||g===null||g.length===0||s&&!m)s?u(null):l();else{let T=s?0:i,M=T*4,E=p.clippingState||null;c.value=E,E=u(g,f,M,h);for(let I=0;I!==M;++I)E[I]=t[I];p.clippingState=E,this.numIntersection=v?this.numPlanes:0,this.numPlanes+=T}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,f,h,g){let v=d!==null?d.length:0,m=null;if(v!==0){if(m=c.value,g!==!0||m===null){let p=h+v*4,T=f.matrixWorldInverse;a.getNormalMatrix(T),(m===null||m.length<p)&&(m=new Float32Array(p));for(let M=0,E=h;M!==v;++M,E+=4)o.copy(d[M]).applyMatrix4(T,a),o.normal.toArray(m,E),m[E+3]=o.constant}c.value=m,c.needsUpdate=!0}return e.numPlanes=v,e.numIntersection=0,m}}function wF(n){let e=new WeakMap;function t(o,a){return a===Fd?o.mapping=hs:a===kd&&(o.mapping=ps),o}function i(o){if(o&&o.isTexture){let a=o.mapping;if(a===Fd||a===kd)if(e.has(o)){let c=e.get(o).texture;return t(c,o.mapping)}else{let c=o.image;if(c&&c.height>0){let l=new cd(c.height);return l.fromEquirectangularTexture(n,o),e.set(o,l),o.addEventListener("dispose",r),t(l.texture,o.mapping)}else return null}}return o}function r(o){let a=o.target;a.removeEventListener("dispose",r);let c=e.get(a);c!==void 0&&(e.delete(a),c.dispose())}function s(){e=new WeakMap}return{get:i,dispose:s}}var Wo=4,yS=[.125,.215,.35,.446,.526,.582],Es=20,Hg=new vc,_S=new tt,Gg=null,jg=0,Wg=0,$g=!1,xs=(1+Math.sqrt(5))/2,jo=1/xs,xS=[new L(-xs,jo,0),new L(xs,jo,0),new L(-jo,0,xs),new L(jo,0,xs),new L(0,xs,-jo),new L(0,xs,jo),new L(-1,1,-1),new L(1,1,-1),new L(-1,1,1),new L(1,1,1)],Mf=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._lodPlanes=[],this._sizeLods=[],this._sigmas=[],this._blurMaterial=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._compileMaterial(this._blurMaterial)}fromScene(e,t=0,i=.1,r=100){Gg=this._renderer.getRenderTarget(),jg=this._renderer.getActiveCubeFace(),Wg=this._renderer.getActiveMipmapLevel(),$g=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(256);let s=this._allocateTargets();return s.depthBuffer=!0,this._sceneToCubeUV(e,i,r,s),t>0&&this._blur(s,0,0,t),this._applyPMREM(s),this._cleanup(s),s}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=SS(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=MS(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose()}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodPlanes.length;e++)this._lodPlanes[e].dispose()}_cleanup(e){this._renderer.setRenderTarget(Gg,jg,Wg),this._renderer.xr.enabled=$g,e.scissorTest=!1,xf(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===hs||e.mapping===ps?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),Gg=this._renderer.getRenderTarget(),jg=this._renderer.getActiveCubeFace(),Wg=this._renderer.getActiveMipmapLevel(),$g=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:Qn,minFilter:Qn,generateMipmaps:!1,type:zo,format:Hn,colorSpace:as,depthBuffer:!1},r=ES(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=ES(e,t,i);let{_lodMax:s}=this;({sizeLods:this._sizeLods,lodPlanes:this._lodPlanes,sigmas:this._sigmas}=bF(s)),this._blurMaterial=TF(s,e,t)}return r}_compileMaterial(e){let t=new Gt(this._lodPlanes[0],e);this._renderer.compile(t,Hg)}_sceneToCubeUV(e,t,i,r){let a=new Qt(90,1,t,i),c=[1,-1,1,1,1,1],l=[1,1,1,-1,-1,-1],u=this._renderer,d=u.autoClear,f=u.toneMapping;u.getClearColor(_S),u.toneMapping=ji,u.autoClear=!1;let h=new Er({name:"PMREM.Background",side:sn,depthWrite:!1,depthTest:!1}),g=new Gt(new Oo,h),v=!1,m=e.background;m?m.isColor&&(h.color.copy(m),e.background=null,v=!0):(h.color.copy(_S),v=!0);for(let p=0;p<6;p++){let T=p%3;T===0?(a.up.set(0,c[p],0),a.lookAt(l[p],0,0)):T===1?(a.up.set(0,0,c[p]),a.lookAt(0,l[p],0)):(a.up.set(0,c[p],0),a.lookAt(0,0,l[p]));let M=this._cubeSize;xf(r,T*M,p>2?M:0,M,M),u.setRenderTarget(r),v&&u.render(g,a),u.render(e,a)}g.geometry.dispose(),g.material.dispose(),u.toneMapping=f,u.autoClear=d,e.background=m}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===hs||e.mapping===ps;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=SS()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=MS());let s=r?this._cubemapMaterial:this._equirectMaterial,o=new Gt(this._lodPlanes[0],s),a=s.uniforms;a.envMap.value=e;let c=this._cubeSize;xf(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(o,Hg)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodPlanes.length;for(let s=1;s<r;s++){let o=Math.sqrt(this._sigmas[s]*this._sigmas[s]-this._sigmas[s-1]*this._sigmas[s-1]),a=xS[(r-s-1)%xS.length];this._blur(e,s-1,s,o,a)}t.autoClear=i}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let c=this._renderer,l=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&console.error("blur direction must be either latitudinal or longitudinal!");let u=3,d=new Gt(this._lodPlanes[r],l),f=l.uniforms,h=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*h):2*Math.PI/(2*Es-1),v=s/g,m=isFinite(s)?1+Math.floor(u*v):Es;m>Es&&console.warn(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${Es}`);let p=[],T=0;for(let C=0;C<Es;++C){let N=C/v,w=Math.exp(-N*N/2);p.push(w),C===0?T+=w:C<m&&(T+=2*w)}for(let C=0;C<p.length;C++)p[C]=p[C]/T;f.envMap.value=e.texture,f.samples.value=m,f.weights.value=p,f.latitudinal.value=o==="latitudinal",a&&(f.poleAxis.value=a);let{_lodMax:M}=this;f.dTheta.value=g,f.mipInt.value=M-i;let E=this._sizeLods[r],I=3*E*(r>M-Wo?r-M+Wo:0),D=4*(this._cubeSize-E);xf(t,I,D,3*E,2*E),c.setRenderTarget(t),c.render(d,Hg)}};function bF(n){let e=[],t=[],i=[],r=n,s=n-Wo+1+yS.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);t.push(a);let c=1/a;o>n-Wo?c=yS[o-n+Wo-1]:o===0&&(c=0),i.push(c);let l=1/(a-2),u=-l,d=1+l,f=[u,u,d,u,d,d,u,u,d,d,u,d],h=6,g=6,v=3,m=2,p=1,T=new Float32Array(v*g*h),M=new Float32Array(m*g*h),E=new Float32Array(p*g*h);for(let D=0;D<h;D++){let C=D%3*2/3-1,N=D>2?0:-1,w=[C,N,0,C+2/3,N,0,C+2/3,N+1,0,C,N,0,C+2/3,N+1,0,C,N+1,0];T.set(w,v*g*D),M.set(f,m*g*D);let x=[D,D,D,D,D,D];E.set(x,p*g*D)}let I=new fi;I.setAttribute("position",new Cn(T,v)),I.setAttribute("uv",new Cn(M,m)),I.setAttribute("faceIndex",new Cn(E,p)),e.push(I),r>Wo&&r--}return{lodPlanes:e,sizeLods:t,sigmas:i}}function ES(n,e,t){let i=new ui(n,e,t);return i.texture.mapping=Ec,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function xf(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function TF(n,e,t){let i=new Float32Array(Es),r=new L(0,1,0);return new ei({name:"SphericalGaussianBlur",defines:{n:Es,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:nv(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:Gi,depthTest:!1,depthWrite:!1})}function MS(){return new ei({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:nv(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:Gi,depthTest:!1,depthWrite:!1})}function SS(){return new ei({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:nv(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:Gi,depthTest:!1,depthWrite:!1})}function nv(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}function CF(n){let e=new WeakMap,t=null;function i(a){if(a&&a.isTexture){let c=a.mapping,l=c===Fd||c===kd,u=c===hs||c===ps;if(l||u){let d=e.get(a),f=d!==void 0?d.texture.pmremVersion:0;if(a.isRenderTargetTexture&&a.pmremVersion!==f)return t===null&&(t=new Mf(n)),d=l?t.fromEquirectangular(a,d):t.fromCubemap(a,d),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),d.texture;if(d!==void 0)return d.texture;{let h=a.image;return l&&h&&h.height>0||u&&h&&r(h)?(t===null&&(t=new Mf(n)),d=l?t.fromEquirectangular(a):t.fromCubemap(a),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),a.addEventListener("dispose",s),d.texture):null}}}return a}function r(a){let c=0,l=6;for(let u=0;u<l;u++)a[u]!==void 0&&c++;return c===l}function s(a){let c=a.target;c.removeEventListener("dispose",s);let l=e.get(c);l!==void 0&&(e.delete(c),l.dispose())}function o(){e=new WeakMap,t!==null&&(t.dispose(),t=null)}return{get:i,dispose:o}}function DF(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r;switch(i){case"WEBGL_depth_texture":r=n.getExtension("WEBGL_depth_texture")||n.getExtension("MOZ_WEBGL_depth_texture")||n.getExtension("WEBKIT_WEBGL_depth_texture");break;case"EXT_texture_filter_anisotropic":r=n.getExtension("EXT_texture_filter_anisotropic")||n.getExtension("MOZ_EXT_texture_filter_anisotropic")||n.getExtension("WEBKIT_EXT_texture_filter_anisotropic");break;case"WEBGL_compressed_texture_s3tc":r=n.getExtension("WEBGL_compressed_texture_s3tc")||n.getExtension("MOZ_WEBGL_compressed_texture_s3tc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_s3tc");break;case"WEBGL_compressed_texture_pvrtc":r=n.getExtension("WEBGL_compressed_texture_pvrtc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_pvrtc");break;default:r=n.getExtension(i)}return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&gs("THREE.WebGLRenderer: "+i+" extension not supported."),r}}}function AF(n,e,t,i){let r={},s=new WeakMap;function o(d){let f=d.target;f.index!==null&&e.remove(f.index);for(let g in f.attributes)e.remove(f.attributes[g]);f.removeEventListener("dispose",o),delete r[f.id];let h=s.get(f);h&&(e.remove(h),s.delete(f)),i.releaseStatesOfGeometry(f),f.isInstancedBufferGeometry===!0&&delete f._maxInstanceCount,t.memory.geometries--}function a(d,f){return r[f.id]===!0||(f.addEventListener("dispose",o),r[f.id]=!0,t.memory.geometries++),f}function c(d){let f=d.attributes;for(let h in f)e.update(f[h],n.ARRAY_BUFFER)}function l(d){let f=[],h=d.index,g=d.attributes.position,v=0;if(h!==null){let T=h.array;v=h.version;for(let M=0,E=T.length;M<E;M+=3){let I=T[M+0],D=T[M+1],C=T[M+2];f.push(I,D,D,C,C,I)}}else if(g!==void 0){let T=g.array;v=g.version;for(let M=0,E=T.length/3-1;M<E;M+=3){let I=M+0,D=M+1,C=M+2;f.push(I,D,D,C,C,I)}}else return;let m=new(Fg(f)?ec:Qa)(f,1);m.version=v;let p=s.get(d);p&&e.remove(p),s.set(d,m)}function u(d){let f=s.get(d);if(f){let h=d.index;h!==null&&f.version<h.version&&l(d)}else l(d);return s.get(d)}return{get:a,update:c,getWireframeAttribute:u}}function IF(n,e,t){let i;function r(f){i=f}let s,o;function a(f){s=f.type,o=f.bytesPerElement}function c(f,h){n.drawElements(i,h,s,f*o),t.update(h,i,1)}function l(f,h,g){g!==0&&(n.drawElementsInstanced(i,h,s,f*o,g),t.update(h,i,g))}function u(f,h,g){if(g===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,h,0,s,f,0,g);let m=0;for(let p=0;p<g;p++)m+=h[p];t.update(m,i,1)}function d(f,h,g,v){if(g===0)return;let m=e.get("WEBGL_multi_draw");if(m===null)for(let p=0;p<f.length;p++)l(f[p]/o,h[p],v[p]);else{m.multiDrawElementsInstancedWEBGL(i,h,0,s,f,0,v,0,g);let p=0;for(let T=0;T<g;T++)p+=h[T]*v[T];t.update(p,i,1)}}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u,this.renderMultiDrawInstances=d}function RF(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:console.error("THREE.WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function NF(n,e,t){let i=new WeakMap,r=new Tt;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,f=i.get(a);if(f===void 0||f.count!==d){let x=function(){N.dispose(),i.delete(a),a.removeEventListener("dispose",x)};var h=x;f!==void 0&&f.texture.dispose();let g=a.morphAttributes.position!==void 0,v=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],T=a.morphAttributes.normal||[],M=a.morphAttributes.color||[],E=0;g===!0&&(E=1),v===!0&&(E=2),m===!0&&(E=3);let I=a.attributes.position.count*E,D=1;I>e.maxTextureSize&&(D=Math.ceil(I/e.maxTextureSize),I=e.maxTextureSize);let C=new Float32Array(I*D*4*d),N=new Ka(C,I,D,d);N.type=gi,N.needsUpdate=!0;let w=E*4;for(let A=0;A<d;A++){let $=p[A],B=T[A],q=M[A],X=I*D*4*A;for(let H=0;H<$.count;H++){let Z=H*w;g===!0&&(r.fromBufferAttribute($,H),C[X+Z+0]=r.x,C[X+Z+1]=r.y,C[X+Z+2]=r.z,C[X+Z+3]=0),v===!0&&(r.fromBufferAttribute(B,H),C[X+Z+4]=r.x,C[X+Z+5]=r.y,C[X+Z+6]=r.z,C[X+Z+7]=0),m===!0&&(r.fromBufferAttribute(q,H),C[X+Z+8]=r.x,C[X+Z+9]=r.y,C[X+Z+10]=r.z,C[X+Z+11]=q.itemSize===4?r.w:1)}}f={count:d,texture:N,size:new fe(I,D)},i.set(a,f),a.addEventListener("dispose",x)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<l.length;m++)g+=l[m];let v=a.morphTargetsRelative?1:1-g;c.getUniforms().setValue(n,"morphTargetBaseInfluence",v),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",f.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",f.size)}return{update:s}}function PF(n,e,t,i){let r=new WeakMap;function s(c){let l=i.render.frame,u=c.geometry,d=e.get(c,u);if(r.get(d)!==l&&(e.update(d),r.set(d,l)),c.isInstancedMesh&&(c.hasEventListener("dispose",a)===!1&&c.addEventListener("dispose",a),r.get(c)!==l&&(t.update(c.instanceMatrix,n.ARRAY_BUFFER),c.instanceColor!==null&&t.update(c.instanceColor,n.ARRAY_BUFFER),r.set(c,l))),c.isSkinnedMesh){let f=c.skeleton;r.get(f)!==l&&(f.update(),r.set(f,l))}return d}function o(){r=new WeakMap}function a(c){let l=c.target;l.removeEventListener("dispose",a),t.remove(l.instanceMatrix),l.instanceColor!==null&&t.remove(l.instanceColor)}return{update:s,dispose:o}}var HS=new $i,wS=new rc(1,1),GS=new Ka,jS=new sd,WS=new nc,bS=[],TS=[],CS=new Float32Array(16),DS=new Float32Array(9),AS=new Float32Array(4);function qo(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=bS[r];if(s===void 0&&(s=new Float32Array(r),bS[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Ut(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Bt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function wf(n,e){let t=TS[e];t===void 0&&(t=new Int32Array(e),TS[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function OF(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function LF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Ut(t,e))return;n.uniform2fv(this.addr,e),Bt(t,e)}}function FF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Ut(t,e))return;n.uniform3fv(this.addr,e),Bt(t,e)}}function kF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Ut(t,e))return;n.uniform4fv(this.addr,e),Bt(t,e)}}function UF(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Ut(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Bt(t,e)}else{if(Ut(t,i))return;AS.set(i),n.uniformMatrix2fv(this.addr,!1,AS),Bt(t,i)}}function BF(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Ut(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Bt(t,e)}else{if(Ut(t,i))return;DS.set(i),n.uniformMatrix3fv(this.addr,!1,DS),Bt(t,i)}}function VF(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Ut(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Bt(t,e)}else{if(Ut(t,i))return;CS.set(i),n.uniformMatrix4fv(this.addr,!1,CS),Bt(t,i)}}function zF(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function HF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Ut(t,e))return;n.uniform2iv(this.addr,e),Bt(t,e)}}function GF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Ut(t,e))return;n.uniform3iv(this.addr,e),Bt(t,e)}}function jF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Ut(t,e))return;n.uniform4iv(this.addr,e),Bt(t,e)}}function WF(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function $F(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Ut(t,e))return;n.uniform2uiv(this.addr,e),Bt(t,e)}}function qF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Ut(t,e))return;n.uniform3uiv(this.addr,e),Bt(t,e)}}function XF(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Ut(t,e))return;n.uniform4uiv(this.addr,e),Bt(t,e)}}function YF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(wS.compareFunction=Og,s=wS):s=HS,t.setTexture2D(e||s,r)}function ZF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||jS,r)}function KF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||WS,r)}function JF(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||GS,r)}function QF(n){switch(n){case 5126:return OF;case 35664:return LF;case 35665:return FF;case 35666:return kF;case 35674:return UF;case 35675:return BF;case 35676:return VF;case 5124:case 35670:return zF;case 35667:case 35671:return HF;case 35668:case 35672:return GF;case 35669:case 35673:return jF;case 5125:return WF;case 36294:return $F;case 36295:return qF;case 36296:return XF;case 35678:case 36198:case 36298:case 36306:case 35682:return YF;case 35679:case 36299:case 36307:return ZF;case 35680:case 36300:case 36308:case 36293:return KF;case 36289:case 36303:case 36311:case 36292:return JF}}function ek(n,e){n.uniform1fv(this.addr,e)}function tk(n,e){let t=qo(e,this.size,2);n.uniform2fv(this.addr,t)}function nk(n,e){let t=qo(e,this.size,3);n.uniform3fv(this.addr,t)}function ik(n,e){let t=qo(e,this.size,4);n.uniform4fv(this.addr,t)}function rk(n,e){let t=qo(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function sk(n,e){let t=qo(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function ok(n,e){let t=qo(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function ak(n,e){n.uniform1iv(this.addr,e)}function ck(n,e){n.uniform2iv(this.addr,e)}function lk(n,e){n.uniform3iv(this.addr,e)}function uk(n,e){n.uniform4iv(this.addr,e)}function dk(n,e){n.uniform1uiv(this.addr,e)}function fk(n,e){n.uniform2uiv(this.addr,e)}function hk(n,e){n.uniform3uiv(this.addr,e)}function pk(n,e){n.uniform4uiv(this.addr,e)}function mk(n,e,t){let i=this.cache,r=e.length,s=wf(t,r);Ut(i,s)||(n.uniform1iv(this.addr,s),Bt(i,s));for(let o=0;o!==r;++o)t.setTexture2D(e[o]||HS,s[o])}function gk(n,e,t){let i=this.cache,r=e.length,s=wf(t,r);Ut(i,s)||(n.uniform1iv(this.addr,s),Bt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||jS,s[o])}function vk(n,e,t){let i=this.cache,r=e.length,s=wf(t,r);Ut(i,s)||(n.uniform1iv(this.addr,s),Bt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||WS,s[o])}function yk(n,e,t){let i=this.cache,r=e.length,s=wf(t,r);Ut(i,s)||(n.uniform1iv(this.addr,s),Bt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||GS,s[o])}function _k(n){switch(n){case 5126:return ek;case 35664:return tk;case 35665:return nk;case 35666:return ik;case 35674:return rk;case 35675:return sk;case 35676:return ok;case 5124:case 35670:return ak;case 35667:case 35671:return ck;case 35668:case 35672:return lk;case 35669:case 35673:return uk;case 5125:return dk;case 36294:return fk;case 36295:return hk;case 36296:return pk;case 35678:case 36198:case 36298:case 36306:case 35682:return mk;case 35679:case 36299:case 36307:return gk;case 35680:case 36300:case 36308:case 36293:return vk;case 36289:case 36303:case 36311:case 36292:return yk}}var Xg=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=QF(t.type)}},Yg=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=_k(t.type)}},Zg=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},qg=/(\w+)(\])?(\[|\.)?/g;function IS(n,e){n.seq.push(e),n.map[e.id]=e}function xk(n,e,t){let i=n.name,r=i.length;for(qg.lastIndex=0;;){let s=qg.exec(i),o=qg.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){IS(t,l===void 0?new Xg(a,n,e):new Yg(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new Zg(a),IS(t,d)),t=d}}}var $o=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let r=0;r<i;++r){let s=e.getActiveUniform(t,r),o=e.getUniformLocation(t,s.name);xk(s,o,this)}}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function RS(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var Ek=37297,Mk=0;function Sk(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var NS=new je;function wk(n){ct._getMatrix(NS,ct.workingColorSpace,n);let e=`mat3( ${NS.elements.map(t=>t.toFixed(4))} )`;switch(ct.getTransfer(n)){case Xa:return[e,"LinearTransferOETF"];case _t:return[e,"sRGBTransferOETF"];default:return console.warn("THREE.WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function PS(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),r=n.getShaderInfoLog(e).trim();if(i&&r==="")return"";let s=/ERROR: 0:(\d+)/.exec(r);if(s){let o=parseInt(s[1]);return t.toUpperCase()+`

`+r+`

`+Sk(n.getShaderSource(e),o)}else return r}function bk(n,e){let t=wk(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}function Tk(n,e){let t;switch(e){case jM:t="Linear";break;case WM:t="Reinhard";break;case $M:t="Cineon";break;case qM:t="ACESFilmic";break;case YM:t="AgX";break;case ZM:t="Neutral";break;case XM:t="Custom";break;default:console.warn("THREE.WebGLProgram: Unsupported toneMapping:",e),t="Linear"}return"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var Ef=new L;function Ck(){ct.getLuminanceCoefficients(Ef);let n=Ef.x.toFixed(4),e=Ef.y.toFixed(4),t=Ef.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function Dk(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(Dc).join(`
`)}function Ak(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function Ik(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function Dc(n){return n!==""}function OS(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function LS(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var Rk=/^[ \t]*#include +<([\w\d./]+)>/gm;function Kg(n){return n.replace(Rk,Pk)}var Nk=new Map;function Pk(n,e){let t=Ze[e];if(t===void 0){let i=Nk.get(e);if(i!==void 0)t=Ze[i],console.warn('THREE.WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return Kg(t)}var Ok=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function FS(n){return n.replace(Ok,Lk)}function Lk(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function kS(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}function Fk(n){let e="SHADOWMAP_TYPE_BASIC";return n.shadowMapType===_g?e="SHADOWMAP_TYPE_PCF":n.shadowMapType===SM?e="SHADOWMAP_TYPE_PCF_SOFT":n.shadowMapType===hi&&(e="SHADOWMAP_TYPE_VSM"),e}function kk(n){let e="ENVMAP_TYPE_CUBE";if(n.envMap)switch(n.envMapMode){case hs:case ps:e="ENVMAP_TYPE_CUBE";break;case Ec:e="ENVMAP_TYPE_CUBE_UV";break}return e}function Uk(n){let e="ENVMAP_MODE_REFLECTION";if(n.envMap)switch(n.envMapMode){case ps:e="ENVMAP_MODE_REFRACTION";break}return e}function Bk(n){let e="ENVMAP_BLENDING_NONE";if(n.envMap)switch(n.combine){case Ld:e="ENVMAP_BLENDING_MULTIPLY";break;case HM:e="ENVMAP_BLENDING_MIX";break;case GM:e="ENVMAP_BLENDING_ADD";break}return e}function Vk(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),7*16)),texelHeight:i,maxMip:t}}function zk(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=Fk(t),l=kk(t),u=Uk(t),d=Bk(t),f=Vk(t),h=Dk(t),g=Ak(s),v=r.createProgram(),m,p,T=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Dc).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Dc).join(`
`),p.length>0&&(p+=`
`)):(m=[kS(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(Dc).join(`
`),p=[kS(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",f?"#define CUBEUV_TEXEL_WIDTH "+f.texelWidth:"",f?"#define CUBEUV_TEXEL_HEIGHT "+f.texelHeight:"",f?"#define CUBEUV_MAX_MIP "+f.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor||t.batchingColor?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==ji?"#define TONE_MAPPING":"",t.toneMapping!==ji?Ze.tonemapping_pars_fragment:"",t.toneMapping!==ji?Tk("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",Ze.colorspace_pars_fragment,bk("linearToOutputTexel",t.outputColorSpace),Ck(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(Dc).join(`
`)),o=Kg(o),o=OS(o,t),o=LS(o,t),a=Kg(a),a=OS(a,t),a=LS(a,t),o=FS(o),a=FS(a),t.isRawShaderMaterial!==!0&&(T=`#version 300 es
`,m=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===Lg?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===Lg?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let M=T+m+o,E=T+p+a,I=RS(r,r.VERTEX_SHADER,M),D=RS(r,r.FRAGMENT_SHADER,E);r.attachShader(v,I),r.attachShader(v,D),t.index0AttributeName!==void 0?r.bindAttribLocation(v,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(v,0,"position"),r.linkProgram(v);function C(A){if(n.debug.checkShaderErrors){let $=r.getProgramInfoLog(v).trim(),B=r.getShaderInfoLog(I).trim(),q=r.getShaderInfoLog(D).trim(),X=!0,H=!0;if(r.getProgramParameter(v,r.LINK_STATUS)===!1)if(X=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,v,I,D);else{let Z=PS(r,I,"vertex"),V=PS(r,D,"fragment");console.error("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(v,r.VALIDATE_STATUS)+`

Material Name: `+A.name+`
Material Type: `+A.type+`

Program Info Log: `+$+`
`+Z+`
`+V)}else $!==""?console.warn("THREE.WebGLProgram: Program Info Log:",$):(B===""||q==="")&&(H=!1);H&&(A.diagnostics={runnable:X,programLog:$,vertexShader:{log:B,prefix:m},fragmentShader:{log:q,prefix:p}})}r.deleteShader(I),r.deleteShader(D),N=new $o(r,v),w=Ik(r,v)}let N;this.getUniforms=function(){return N===void 0&&C(this),N};let w;this.getAttributes=function(){return w===void 0&&C(this),w};let x=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return x===!1&&(x=r.getProgramParameter(v,Ek)),x},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(v),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=Mk++,this.cacheKey=e,this.usedTimes=1,this.program=v,this.vertexShader=I,this.fragmentShader=D,this}var Hk=0,Jg=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new Qg(e),t.set(e,i)),i}},Qg=class{constructor(e){this.id=Hk++,this.code=e,this.usedTimes=0}};function Gk(n,e,t,i,r,s,o){let a=new Ja,c=new Jg,l=new Set,u=[],d=r.logarithmicDepthBuffer,f=r.vertexTextures,h=r.precision,g={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distanceRGBA",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function v(w){return l.add(w),w===0?"uv":`uv${w}`}function m(w,x,A,$,B){let q=$.fog,X=B.geometry,H=w.isMeshStandardMaterial?$.environment:null,Z=(w.isMeshStandardMaterial?t:e).get(w.envMap||H),V=Z&&Z.mapping===Ec?Z.image.height:null,ae=g[w.type];w.precision!==null&&(h=r.getMaxPrecision(w.precision),h!==w.precision&&console.warn("THREE.WebGLProgram.getParameters:",w.precision,"not supported, using",h,"instead."));let ge=X.morphAttributes.position||X.morphAttributes.normal||X.morphAttributes.color,Se=ge!==void 0?ge.length:0,Ge=0;X.morphAttributes.position!==void 0&&(Ge=1),X.morphAttributes.normal!==void 0&&(Ge=2),X.morphAttributes.color!==void 0&&(Ge=3);let dt,W,se,we;if(ae){let mt=vi[ae];dt=mt.vertexShader,W=mt.fragmentShader}else dt=w.vertexShader,W=w.fragmentShader,c.update(w),se=c.getVertexShaderID(w),we=c.getFragmentShaderID(w);let oe=n.getRenderTarget(),Ie=n.state.buffers.depth.getReversed(),ot=B.isInstancedMesh===!0,De=B.isBatchedMesh===!0,ut=!!w.map,ee=!!w.matcap,J=!!Z,b=!!w.aoMap,Ce=!!w.lightMap,te=!!w.bumpMap,ye=!!w.normalMap,re=!!w.displacementMap,Fe=!!w.emissiveMap,he=!!w.metalnessMap,S=!!w.roughnessMap,y=w.anisotropy>0,F=w.clearcoat>0,G=w.dispersion>0,K=w.iridescence>0,j=w.sheen>0,Te=w.transmission>0,le=y&&!!w.anisotropyMap,ve=F&&!!w.clearcoatMap,Qe=F&&!!w.clearcoatNormalMap,ie=F&&!!w.clearcoatRoughnessMap,xe=K&&!!w.iridescenceMap,Le=K&&!!w.iridescenceThicknessMap,Ue=j&&!!w.sheenColorMap,Ee=j&&!!w.sheenRoughnessMap,it=!!w.specularMap,Ye=!!w.specularColorMap,Et=!!w.specularIntensityMap,R=Te&&!!w.transmissionMap,ue=Te&&!!w.thicknessMap,z=!!w.gradientMap,Y=!!w.alphaMap,me=w.alphaTest>0,pe=!!w.alphaHash,We=!!w.extensions,Ct=ji;w.toneMapped&&(oe===null||oe.isXRRenderTarget===!0)&&(Ct=n.toneMapping);let Xt={shaderID:ae,shaderType:w.type,shaderName:w.name,vertexShader:dt,fragmentShader:W,defines:w.defines,customVertexShaderID:se,customFragmentShaderID:we,isRawShaderMaterial:w.isRawShaderMaterial===!0,glslVersion:w.glslVersion,precision:h,batching:De,batchingColor:De&&B._colorsTexture!==null,instancing:ot,instancingColor:ot&&B.instanceColor!==null,instancingMorph:ot&&B.morphTexture!==null,supportsVertexTextures:f,outputColorSpace:oe===null?n.outputColorSpace:oe.isXRRenderTarget===!0?oe.texture.colorSpace:as,alphaToCoverage:!!w.alphaToCoverage,map:ut,matcap:ee,envMap:J,envMapMode:J&&Z.mapping,envMapCubeUVHeight:V,aoMap:b,lightMap:Ce,bumpMap:te,normalMap:ye,displacementMap:f&&re,emissiveMap:Fe,normalMapObjectSpace:ye&&w.normalMapType===eS,normalMapTangentSpace:ye&&w.normalMapType===Pg,metalnessMap:he,roughnessMap:S,anisotropy:y,anisotropyMap:le,clearcoat:F,clearcoatMap:ve,clearcoatNormalMap:Qe,clearcoatRoughnessMap:ie,dispersion:G,iridescence:K,iridescenceMap:xe,iridescenceThicknessMap:Le,sheen:j,sheenColorMap:Ue,sheenRoughnessMap:Ee,specularMap:it,specularColorMap:Ye,specularIntensityMap:Et,transmission:Te,transmissionMap:R,thicknessMap:ue,gradientMap:z,opaque:w.transparent===!1&&w.blending===rs&&w.alphaToCoverage===!1,alphaMap:Y,alphaTest:me,alphaHash:pe,combine:w.combine,mapUv:ut&&v(w.map.channel),aoMapUv:b&&v(w.aoMap.channel),lightMapUv:Ce&&v(w.lightMap.channel),bumpMapUv:te&&v(w.bumpMap.channel),normalMapUv:ye&&v(w.normalMap.channel),displacementMapUv:re&&v(w.displacementMap.channel),emissiveMapUv:Fe&&v(w.emissiveMap.channel),metalnessMapUv:he&&v(w.metalnessMap.channel),roughnessMapUv:S&&v(w.roughnessMap.channel),anisotropyMapUv:le&&v(w.anisotropyMap.channel),clearcoatMapUv:ve&&v(w.clearcoatMap.channel),clearcoatNormalMapUv:Qe&&v(w.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:ie&&v(w.clearcoatRoughnessMap.channel),iridescenceMapUv:xe&&v(w.iridescenceMap.channel),iridescenceThicknessMapUv:Le&&v(w.iridescenceThicknessMap.channel),sheenColorMapUv:Ue&&v(w.sheenColorMap.channel),sheenRoughnessMapUv:Ee&&v(w.sheenRoughnessMap.channel),specularMapUv:it&&v(w.specularMap.channel),specularColorMapUv:Ye&&v(w.specularColorMap.channel),specularIntensityMapUv:Et&&v(w.specularIntensityMap.channel),transmissionMapUv:R&&v(w.transmissionMap.channel),thicknessMapUv:ue&&v(w.thicknessMap.channel),alphaMapUv:Y&&v(w.alphaMap.channel),vertexTangents:!!X.attributes.tangent&&(ye||y),vertexColors:w.vertexColors,vertexAlphas:w.vertexColors===!0&&!!X.attributes.color&&X.attributes.color.itemSize===4,pointsUvs:B.isPoints===!0&&!!X.attributes.uv&&(ut||Y),fog:!!q,useFog:w.fog===!0,fogExp2:!!q&&q.isFogExp2,flatShading:w.flatShading===!0,sizeAttenuation:w.sizeAttenuation===!0,logarithmicDepthBuffer:d,reverseDepthBuffer:Ie,skinning:B.isSkinnedMesh===!0,morphTargets:X.morphAttributes.position!==void 0,morphNormals:X.morphAttributes.normal!==void 0,morphColors:X.morphAttributes.color!==void 0,morphTargetsCount:Se,morphTextureStride:Ge,numDirLights:x.directional.length,numPointLights:x.point.length,numSpotLights:x.spot.length,numSpotLightMaps:x.spotLightMap.length,numRectAreaLights:x.rectArea.length,numHemiLights:x.hemi.length,numDirLightShadows:x.directionalShadowMap.length,numPointLightShadows:x.pointShadowMap.length,numSpotLightShadows:x.spotShadowMap.length,numSpotLightShadowsWithMaps:x.numSpotLightShadowsWithMaps,numLightProbes:x.numLightProbes,numClippingPlanes:o.numPlanes,numClipIntersection:o.numIntersection,dithering:w.dithering,shadowMapEnabled:n.shadowMap.enabled&&A.length>0,shadowMapType:n.shadowMap.type,toneMapping:Ct,decodeVideoTexture:ut&&w.map.isVideoTexture===!0&&ct.getTransfer(w.map.colorSpace)===_t,decodeVideoTextureEmissive:Fe&&w.emissiveMap.isVideoTexture===!0&&ct.getTransfer(w.emissiveMap.colorSpace)===_t,premultipliedAlpha:w.premultipliedAlpha,doubleSided:w.side===pi,flipSided:w.side===sn,useDepthPacking:w.depthPacking>=0,depthPacking:w.depthPacking||0,index0AttributeName:w.index0AttributeName,extensionClipCullDistance:We&&w.extensions.clipCullDistance===!0&&i.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(We&&w.extensions.multiDraw===!0||De)&&i.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:i.has("KHR_parallel_shader_compile"),customProgramCacheKey:w.customProgramCacheKey()};return Xt.vertexUv1s=l.has(1),Xt.vertexUv2s=l.has(2),Xt.vertexUv3s=l.has(3),l.clear(),Xt}function p(w){let x=[];if(w.shaderID?x.push(w.shaderID):(x.push(w.customVertexShaderID),x.push(w.customFragmentShaderID)),w.defines!==void 0)for(let A in w.defines)x.push(A),x.push(w.defines[A]);return w.isRawShaderMaterial===!1&&(T(x,w),M(x,w),x.push(n.outputColorSpace)),x.push(w.customProgramCacheKey),x.join()}function T(w,x){w.push(x.precision),w.push(x.outputColorSpace),w.push(x.envMapMode),w.push(x.envMapCubeUVHeight),w.push(x.mapUv),w.push(x.alphaMapUv),w.push(x.lightMapUv),w.push(x.aoMapUv),w.push(x.bumpMapUv),w.push(x.normalMapUv),w.push(x.displacementMapUv),w.push(x.emissiveMapUv),w.push(x.metalnessMapUv),w.push(x.roughnessMapUv),w.push(x.anisotropyMapUv),w.push(x.clearcoatMapUv),w.push(x.clearcoatNormalMapUv),w.push(x.clearcoatRoughnessMapUv),w.push(x.iridescenceMapUv),w.push(x.iridescenceThicknessMapUv),w.push(x.sheenColorMapUv),w.push(x.sheenRoughnessMapUv),w.push(x.specularMapUv),w.push(x.specularColorMapUv),w.push(x.specularIntensityMapUv),w.push(x.transmissionMapUv),w.push(x.thicknessMapUv),w.push(x.combine),w.push(x.fogExp2),w.push(x.sizeAttenuation),w.push(x.morphTargetsCount),w.push(x.morphAttributeCount),w.push(x.numDirLights),w.push(x.numPointLights),w.push(x.numSpotLights),w.push(x.numSpotLightMaps),w.push(x.numHemiLights),w.push(x.numRectAreaLights),w.push(x.numDirLightShadows),w.push(x.numPointLightShadows),w.push(x.numSpotLightShadows),w.push(x.numSpotLightShadowsWithMaps),w.push(x.numLightProbes),w.push(x.shadowMapType),w.push(x.toneMapping),w.push(x.numClippingPlanes),w.push(x.numClipIntersection),w.push(x.depthPacking)}function M(w,x){a.disableAll(),x.supportsVertexTextures&&a.enable(0),x.instancing&&a.enable(1),x.instancingColor&&a.enable(2),x.instancingMorph&&a.enable(3),x.matcap&&a.enable(4),x.envMap&&a.enable(5),x.normalMapObjectSpace&&a.enable(6),x.normalMapTangentSpace&&a.enable(7),x.clearcoat&&a.enable(8),x.iridescence&&a.enable(9),x.alphaTest&&a.enable(10),x.vertexColors&&a.enable(11),x.vertexAlphas&&a.enable(12),x.vertexUv1s&&a.enable(13),x.vertexUv2s&&a.enable(14),x.vertexUv3s&&a.enable(15),x.vertexTangents&&a.enable(16),x.anisotropy&&a.enable(17),x.alphaHash&&a.enable(18),x.batching&&a.enable(19),x.dispersion&&a.enable(20),x.batchingColor&&a.enable(21),w.push(a.mask),a.disableAll(),x.fog&&a.enable(0),x.useFog&&a.enable(1),x.flatShading&&a.enable(2),x.logarithmicDepthBuffer&&a.enable(3),x.reverseDepthBuffer&&a.enable(4),x.skinning&&a.enable(5),x.morphTargets&&a.enable(6),x.morphNormals&&a.enable(7),x.morphColors&&a.enable(8),x.premultipliedAlpha&&a.enable(9),x.shadowMapEnabled&&a.enable(10),x.doubleSided&&a.enable(11),x.flipSided&&a.enable(12),x.useDepthPacking&&a.enable(13),x.dithering&&a.enable(14),x.transmission&&a.enable(15),x.sheen&&a.enable(16),x.opaque&&a.enable(17),x.pointsUvs&&a.enable(18),x.decodeVideoTexture&&a.enable(19),x.decodeVideoTextureEmissive&&a.enable(20),x.alphaToCoverage&&a.enable(21),w.push(a.mask)}function E(w){let x=g[w.type],A;if(x){let $=vi[x];A=hS.clone($.uniforms)}else A=w.uniforms;return A}function I(w,x){let A;for(let $=0,B=u.length;$<B;$++){let q=u[$];if(q.cacheKey===x){A=q,++A.usedTimes;break}}return A===void 0&&(A=new zk(n,x,w,s),u.push(A)),A}function D(w){if(--w.usedTimes===0){let x=u.indexOf(w);u[x]=u[u.length-1],u.pop(),w.destroy()}}function C(w){c.remove(w)}function N(){c.dispose()}return{getParameters:m,getProgramCacheKey:p,getUniforms:E,acquireProgram:I,releaseProgram:D,releaseShaderCache:C,programs:u,dispose:N}}function jk(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function Wk(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.z!==e.z?n.z-e.z:n.id-e.id}function US(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function BS(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(d,f,h,g,v,m){let p=n[e];return p===void 0?(p={id:d.id,object:d,geometry:f,material:h,groupOrder:g,renderOrder:d.renderOrder,z:v,group:m},n[e]=p):(p.id=d.id,p.object=d,p.geometry=f,p.material=h,p.groupOrder=g,p.renderOrder=d.renderOrder,p.z=v,p.group=m),e++,p}function a(d,f,h,g,v,m){let p=o(d,f,h,g,v,m);h.transmission>0?i.push(p):h.transparent===!0?r.push(p):t.push(p)}function c(d,f,h,g,v,m){let p=o(d,f,h,g,v,m);h.transmission>0?i.unshift(p):h.transparent===!0?r.unshift(p):t.unshift(p)}function l(d,f){t.length>1&&t.sort(d||Wk),i.length>1&&i.sort(f||US),r.length>1&&r.sort(f||US)}function u(){for(let d=e,f=n.length;d<f;d++){let h=n[d];if(h.id===null)break;h.id=null,h.object=null,h.geometry=null,h.material=null,h.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:a,unshift:c,finish:u,sort:l}}function $k(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new BS,n.set(i,[o])):r>=s.length?(o=new BS,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function qk(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new L,color:new tt};break;case"SpotLight":t={position:new L,direction:new L,color:new tt,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new L,color:new tt,distance:0,decay:0};break;case"HemisphereLight":t={direction:new L,skyColor:new tt,groundColor:new tt};break;case"RectAreaLight":t={color:new tt,position:new L,halfWidth:new L,halfHeight:new L};break}return n[e.id]=t,t}}}function Xk(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new fe};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new fe};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new fe,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var Yk=0;function Zk(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function Kk(n){let e=new qk,t=Xk(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new L);let r=new L,s=new bt,o=new bt;function a(l){let u=0,d=0,f=0;for(let w=0;w<9;w++)i.probe[w].set(0,0,0);let h=0,g=0,v=0,m=0,p=0,T=0,M=0,E=0,I=0,D=0,C=0;l.sort(Zk);for(let w=0,x=l.length;w<x;w++){let A=l[w],$=A.color,B=A.intensity,q=A.distance,X=A.shadow&&A.shadow.map?A.shadow.map.texture:null;if(A.isAmbientLight)u+=$.r*B,d+=$.g*B,f+=$.b*B;else if(A.isLightProbe){for(let H=0;H<9;H++)i.probe[H].addScaledVector(A.sh.coefficients[H],B);C++}else if(A.isDirectionalLight){let H=e.get(A);if(H.color.copy(A.color).multiplyScalar(A.intensity),A.castShadow){let Z=A.shadow,V=t.get(A);V.shadowIntensity=Z.intensity,V.shadowBias=Z.bias,V.shadowNormalBias=Z.normalBias,V.shadowRadius=Z.radius,V.shadowMapSize=Z.mapSize,i.directionalShadow[h]=V,i.directionalShadowMap[h]=X,i.directionalShadowMatrix[h]=A.shadow.matrix,T++}i.directional[h]=H,h++}else if(A.isSpotLight){let H=e.get(A);H.position.setFromMatrixPosition(A.matrixWorld),H.color.copy($).multiplyScalar(B),H.distance=q,H.coneCos=Math.cos(A.angle),H.penumbraCos=Math.cos(A.angle*(1-A.penumbra)),H.decay=A.decay,i.spot[v]=H;let Z=A.shadow;if(A.map&&(i.spotLightMap[I]=A.map,I++,Z.updateMatrices(A),A.castShadow&&D++),i.spotLightMatrix[v]=Z.matrix,A.castShadow){let V=t.get(A);V.shadowIntensity=Z.intensity,V.shadowBias=Z.bias,V.shadowNormalBias=Z.normalBias,V.shadowRadius=Z.radius,V.shadowMapSize=Z.mapSize,i.spotShadow[v]=V,i.spotShadowMap[v]=X,E++}v++}else if(A.isRectAreaLight){let H=e.get(A);H.color.copy($).multiplyScalar(B),H.halfWidth.set(A.width*.5,0,0),H.halfHeight.set(0,A.height*.5,0),i.rectArea[m]=H,m++}else if(A.isPointLight){let H=e.get(A);if(H.color.copy(A.color).multiplyScalar(A.intensity),H.distance=A.distance,H.decay=A.decay,A.castShadow){let Z=A.shadow,V=t.get(A);V.shadowIntensity=Z.intensity,V.shadowBias=Z.bias,V.shadowNormalBias=Z.normalBias,V.shadowRadius=Z.radius,V.shadowMapSize=Z.mapSize,V.shadowCameraNear=Z.camera.near,V.shadowCameraFar=Z.camera.far,i.pointShadow[g]=V,i.pointShadowMap[g]=X,i.pointShadowMatrix[g]=A.shadow.matrix,M++}i.point[g]=H,g++}else if(A.isHemisphereLight){let H=e.get(A);H.skyColor.copy(A.color).multiplyScalar(B),H.groundColor.copy(A.groundColor).multiplyScalar(B),i.hemi[p]=H,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=ce.LTC_FLOAT_1,i.rectAreaLTC2=ce.LTC_FLOAT_2):(i.rectAreaLTC1=ce.LTC_HALF_1,i.rectAreaLTC2=ce.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=f;let N=i.hash;(N.directionalLength!==h||N.pointLength!==g||N.spotLength!==v||N.rectAreaLength!==m||N.hemiLength!==p||N.numDirectionalShadows!==T||N.numPointShadows!==M||N.numSpotShadows!==E||N.numSpotMaps!==I||N.numLightProbes!==C)&&(i.directional.length=h,i.spot.length=v,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=T,i.directionalShadowMap.length=T,i.pointShadow.length=M,i.pointShadowMap.length=M,i.spotShadow.length=E,i.spotShadowMap.length=E,i.directionalShadowMatrix.length=T,i.pointShadowMatrix.length=M,i.spotLightMatrix.length=E+I-D,i.spotLightMap.length=I,i.numSpotLightShadowsWithMaps=D,i.numLightProbes=C,N.directionalLength=h,N.pointLength=g,N.spotLength=v,N.rectAreaLength=m,N.hemiLength=p,N.numDirectionalShadows=T,N.numPointShadows=M,N.numSpotShadows=E,N.numSpotMaps=I,N.numLightProbes=C,i.version=Yk++)}function c(l,u){let d=0,f=0,h=0,g=0,v=0,m=u.matrixWorldInverse;for(let p=0,T=l.length;p<T;p++){let M=l[p];if(M.isDirectionalLight){let E=i.directional[d];E.direction.setFromMatrixPosition(M.matrixWorld),r.setFromMatrixPosition(M.target.matrixWorld),E.direction.sub(r),E.direction.transformDirection(m),d++}else if(M.isSpotLight){let E=i.spot[h];E.position.setFromMatrixPosition(M.matrixWorld),E.position.applyMatrix4(m),E.direction.setFromMatrixPosition(M.matrixWorld),r.setFromMatrixPosition(M.target.matrixWorld),E.direction.sub(r),E.direction.transformDirection(m),h++}else if(M.isRectAreaLight){let E=i.rectArea[g];E.position.setFromMatrixPosition(M.matrixWorld),E.position.applyMatrix4(m),o.identity(),s.copy(M.matrixWorld),s.premultiply(m),o.extractRotation(s),E.halfWidth.set(M.width*.5,0,0),E.halfHeight.set(0,M.height*.5,0),E.halfWidth.applyMatrix4(o),E.halfHeight.applyMatrix4(o),g++}else if(M.isPointLight){let E=i.point[f];E.position.setFromMatrixPosition(M.matrixWorld),E.position.applyMatrix4(m),f++}else if(M.isHemisphereLight){let E=i.hemi[v];E.direction.setFromMatrixPosition(M.matrixWorld),E.direction.transformDirection(m),v++}}}return{setup:a,setupView:c,state:i}}function VS(n){let e=new Kk(n),t=[],i=[];function r(u){l.camera=u,t.length=0,i.length=0}function s(u){t.push(u)}function o(u){i.push(u)}function a(){e.setup(t)}function c(u){e.setupView(t,u)}let l={lightsArray:t,shadowsArray:i,camera:null,lights:e,transmissionRenderTarget:{}};return{init:r,state:l,setupLights:a,setupLightsView:c,pushLight:s,pushShadow:o}}function Jk(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new VS(n),e.set(r,[a])):s>=o.length?(a=new VS(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var Qk=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,e2=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
#include <packing>
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = unpackRGBATo2Half( texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ) );
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = unpackRGBAToDepth( texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ) );
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( squared_mean - mean * mean );
	gl_FragColor = pack2HalfToRGBA( vec2( mean, std_dev ) );
}`;function t2(n,e,t){let i=new Fo,r=new fe,s=new fe,o=new Tt,a=new md({depthPacking:QM}),c=new gd,l={},u=t.maxTextureSize,d={[Vi]:sn,[sn]:Vi,[pi]:pi},f=new ei({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new fe},radius:{value:4}},vertexShader:Qk,fragmentShader:e2}),h=f.clone();h.defines.HORIZONTAL_PASS=1;let g=new fi;g.setAttribute("position",new Cn(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let v=new Gt(g,f),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=_g;let p=this.type;this.render=function(D,C,N){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||D.length===0)return;let w=n.getRenderTarget(),x=n.getActiveCubeFace(),A=n.getActiveMipmapLevel(),$=n.state;$.setBlending(Gi),$.buffers.color.setClear(1,1,1,1),$.buffers.depth.setTest(!0),$.setScissorTest(!1);let B=p!==hi&&this.type===hi,q=p===hi&&this.type!==hi;for(let X=0,H=D.length;X<H;X++){let Z=D[X],V=Z.shadow;if(V===void 0){console.warn("THREE.WebGLShadowMap:",Z,"has no shadow.");continue}if(V.autoUpdate===!1&&V.needsUpdate===!1)continue;r.copy(V.mapSize);let ae=V.getFrameExtents();if(r.multiply(ae),s.copy(V.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/ae.x),r.x=s.x*ae.x,V.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/ae.y),r.y=s.y*ae.y,V.mapSize.y=s.y)),V.map===null||B===!0||q===!0){let Se=this.type!==hi?{minFilter:Vn,magFilter:Vn}:{};V.map!==null&&V.map.dispose(),V.map=new ui(r.x,r.y,Se),V.map.texture.name=Z.name+".shadowMap",V.camera.updateProjectionMatrix()}n.setRenderTarget(V.map),n.clear();let ge=V.getViewportCount();for(let Se=0;Se<ge;Se++){let Ge=V.getViewport(Se);o.set(s.x*Ge.x,s.y*Ge.y,s.x*Ge.z,s.y*Ge.w),$.viewport(o),V.updateMatrices(Z,Se),i=V.getFrustum(),E(C,N,V.camera,Z,this.type)}V.isPointLightShadow!==!0&&this.type===hi&&T(V,N),V.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(w,x,A)};function T(D,C){let N=e.update(v);f.defines.VSM_SAMPLES!==D.blurSamples&&(f.defines.VSM_SAMPLES=D.blurSamples,h.defines.VSM_SAMPLES=D.blurSamples,f.needsUpdate=!0,h.needsUpdate=!0),D.mapPass===null&&(D.mapPass=new ui(r.x,r.y)),f.uniforms.shadow_pass.value=D.map.texture,f.uniforms.resolution.value=D.mapSize,f.uniforms.radius.value=D.radius,n.setRenderTarget(D.mapPass),n.clear(),n.renderBufferDirect(C,null,N,f,v,null),h.uniforms.shadow_pass.value=D.mapPass.texture,h.uniforms.resolution.value=D.mapSize,h.uniforms.radius.value=D.radius,n.setRenderTarget(D.map),n.clear(),n.renderBufferDirect(C,null,N,h,v,null)}function M(D,C,N,w){let x=null,A=N.isPointLight===!0?D.customDistanceMaterial:D.customDepthMaterial;if(A!==void 0)x=A;else if(x=N.isPointLight===!0?c:a,n.localClippingEnabled&&C.clipShadows===!0&&Array.isArray(C.clippingPlanes)&&C.clippingPlanes.length!==0||C.displacementMap&&C.displacementScale!==0||C.alphaMap&&C.alphaTest>0||C.map&&C.alphaTest>0){let $=x.uuid,B=C.uuid,q=l[$];q===void 0&&(q={},l[$]=q);let X=q[B];X===void 0&&(X=x.clone(),q[B]=X,C.addEventListener("dispose",I)),x=X}if(x.visible=C.visible,x.wireframe=C.wireframe,w===hi?x.side=C.shadowSide!==null?C.shadowSide:C.side:x.side=C.shadowSide!==null?C.shadowSide:d[C.side],x.alphaMap=C.alphaMap,x.alphaTest=C.alphaTest,x.map=C.map,x.clipShadows=C.clipShadows,x.clippingPlanes=C.clippingPlanes,x.clipIntersection=C.clipIntersection,x.displacementMap=C.displacementMap,x.displacementScale=C.displacementScale,x.displacementBias=C.displacementBias,x.wireframeLinewidth=C.wireframeLinewidth,x.linewidth=C.linewidth,N.isPointLight===!0&&x.isMeshDistanceMaterial===!0){let $=n.properties.get(x);$.light=N}return x}function E(D,C,N,w,x){if(D.visible===!1)return;if(D.layers.test(C.layers)&&(D.isMesh||D.isLine||D.isPoints)&&(D.castShadow||D.receiveShadow&&x===hi)&&(!D.frustumCulled||i.intersectsObject(D))){D.modelViewMatrix.multiplyMatrices(N.matrixWorldInverse,D.matrixWorld);let B=e.update(D),q=D.material;if(Array.isArray(q)){let X=B.groups;for(let H=0,Z=X.length;H<Z;H++){let V=X[H],ae=q[V.materialIndex];if(ae&&ae.visible){let ge=M(D,ae,w,x);D.onBeforeShadow(n,D,C,N,B,ge,V),n.renderBufferDirect(N,null,B,ge,D,V),D.onAfterShadow(n,D,C,N,B,ge,V)}}}else if(q.visible){let X=M(D,q,w,x);D.onBeforeShadow(n,D,C,N,B,X,null),n.renderBufferDirect(N,null,B,X,D,null),D.onAfterShadow(n,D,C,N,B,X,null)}}let $=D.children;for(let B=0,q=$.length;B<q;B++)E($[B],C,N,w,x)}function I(D){D.target.removeEventListener("dispose",I);for(let N in l){let w=l[N],x=D.target.uuid;x in w&&(w[x].dispose(),delete w[x])}}}var n2={[Dd]:Ad,[Id]:Pd,[Rd]:Od,[ss]:Nd,[Ad]:Dd,[Pd]:Id,[Od]:Rd,[Nd]:ss};function i2(n,e){function t(){let R=!1,ue=new Tt,z=null,Y=new Tt(0,0,0,0);return{setMask:function(me){z!==me&&!R&&(n.colorMask(me,me,me,me),z=me)},setLocked:function(me){R=me},setClear:function(me,pe,We,Ct,Xt){Xt===!0&&(me*=Ct,pe*=Ct,We*=Ct),ue.set(me,pe,We,Ct),Y.equals(ue)===!1&&(n.clearColor(me,pe,We,Ct),Y.copy(ue))},reset:function(){R=!1,z=null,Y.set(-1,0,0,0)}}}function i(){let R=!1,ue=!1,z=null,Y=null,me=null;return{setReversed:function(pe){if(ue!==pe){let We=e.get("EXT_clip_control");ue?We.clipControlEXT(We.LOWER_LEFT_EXT,We.ZERO_TO_ONE_EXT):We.clipControlEXT(We.LOWER_LEFT_EXT,We.NEGATIVE_ONE_TO_ONE_EXT);let Ct=me;me=null,this.setClear(Ct)}ue=pe},getReversed:function(){return ue},setTest:function(pe){pe?oe(n.DEPTH_TEST):Ie(n.DEPTH_TEST)},setMask:function(pe){z!==pe&&!R&&(n.depthMask(pe),z=pe)},setFunc:function(pe){if(ue&&(pe=n2[pe]),Y!==pe){switch(pe){case Dd:n.depthFunc(n.NEVER);break;case Ad:n.depthFunc(n.ALWAYS);break;case Id:n.depthFunc(n.LESS);break;case ss:n.depthFunc(n.LEQUAL);break;case Rd:n.depthFunc(n.EQUAL);break;case Nd:n.depthFunc(n.GEQUAL);break;case Pd:n.depthFunc(n.GREATER);break;case Od:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}Y=pe}},setLocked:function(pe){R=pe},setClear:function(pe){me!==pe&&(ue&&(pe=1-pe),n.clearDepth(pe),me=pe)},reset:function(){R=!1,z=null,Y=null,me=null,ue=!1}}}function r(){let R=!1,ue=null,z=null,Y=null,me=null,pe=null,We=null,Ct=null,Xt=null;return{setTest:function(mt){R||(mt?oe(n.STENCIL_TEST):Ie(n.STENCIL_TEST))},setMask:function(mt){ue!==mt&&!R&&(n.stencilMask(mt),ue=mt)},setFunc:function(mt,Gn,yi){(z!==mt||Y!==Gn||me!==yi)&&(n.stencilFunc(mt,Gn,yi),z=mt,Y=Gn,me=yi)},setOp:function(mt,Gn,yi){(pe!==mt||We!==Gn||Ct!==yi)&&(n.stencilOp(mt,Gn,yi),pe=mt,We=Gn,Ct=yi)},setLocked:function(mt){R=mt},setClear:function(mt){Xt!==mt&&(n.clearStencil(mt),Xt=mt)},reset:function(){R=!1,ue=null,z=null,Y=null,me=null,pe=null,We=null,Ct=null,Xt=null}}}let s=new t,o=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},d={},f=new WeakMap,h=[],g=null,v=!1,m=null,p=null,T=null,M=null,E=null,I=null,D=null,C=new tt(0,0,0),N=0,w=!1,x=null,A=null,$=null,B=null,q=null,X=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),H=!1,Z=0,V=n.getParameter(n.VERSION);V.indexOf("WebGL")!==-1?(Z=parseFloat(/^WebGL (\d)/.exec(V)[1]),H=Z>=1):V.indexOf("OpenGL ES")!==-1&&(Z=parseFloat(/^OpenGL ES (\d)/.exec(V)[1]),H=Z>=2);let ae=null,ge={},Se=n.getParameter(n.SCISSOR_BOX),Ge=n.getParameter(n.VIEWPORT),dt=new Tt().fromArray(Se),W=new Tt().fromArray(Ge);function se(R,ue,z,Y){let me=new Uint8Array(4),pe=n.createTexture();n.bindTexture(R,pe),n.texParameteri(R,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(R,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let We=0;We<z;We++)R===n.TEXTURE_3D||R===n.TEXTURE_2D_ARRAY?n.texImage3D(ue,0,n.RGBA,1,1,Y,0,n.RGBA,n.UNSIGNED_BYTE,me):n.texImage2D(ue+We,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,me);return pe}let we={};we[n.TEXTURE_2D]=se(n.TEXTURE_2D,n.TEXTURE_2D,1),we[n.TEXTURE_CUBE_MAP]=se(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),we[n.TEXTURE_2D_ARRAY]=se(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),we[n.TEXTURE_3D]=se(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),oe(n.DEPTH_TEST),o.setFunc(ss),te(!1),ye(yg),oe(n.CULL_FACE),b(Gi);function oe(R){u[R]!==!0&&(n.enable(R),u[R]=!0)}function Ie(R){u[R]!==!1&&(n.disable(R),u[R]=!1)}function ot(R,ue){return d[R]!==ue?(n.bindFramebuffer(R,ue),d[R]=ue,R===n.DRAW_FRAMEBUFFER&&(d[n.FRAMEBUFFER]=ue),R===n.FRAMEBUFFER&&(d[n.DRAW_FRAMEBUFFER]=ue),!0):!1}function De(R,ue){let z=h,Y=!1;if(R){z=f.get(ue),z===void 0&&(z=[],f.set(ue,z));let me=R.textures;if(z.length!==me.length||z[0]!==n.COLOR_ATTACHMENT0){for(let pe=0,We=me.length;pe<We;pe++)z[pe]=n.COLOR_ATTACHMENT0+pe;z.length=me.length,Y=!0}}else z[0]!==n.BACK&&(z[0]=n.BACK,Y=!0);Y&&n.drawBuffers(z)}function ut(R){return g!==R?(n.useProgram(R),g=R,!0):!1}let ee={[vr]:n.FUNC_ADD,[bM]:n.FUNC_SUBTRACT,[TM]:n.FUNC_REVERSE_SUBTRACT};ee[CM]=n.MIN,ee[DM]=n.MAX;let J={[AM]:n.ZERO,[IM]:n.ONE,[RM]:n.SRC_COLOR,[Ku]:n.SRC_ALPHA,[kM]:n.SRC_ALPHA_SATURATE,[LM]:n.DST_COLOR,[PM]:n.DST_ALPHA,[NM]:n.ONE_MINUS_SRC_COLOR,[Ju]:n.ONE_MINUS_SRC_ALPHA,[FM]:n.ONE_MINUS_DST_COLOR,[OM]:n.ONE_MINUS_DST_ALPHA,[UM]:n.CONSTANT_COLOR,[BM]:n.ONE_MINUS_CONSTANT_COLOR,[VM]:n.CONSTANT_ALPHA,[zM]:n.ONE_MINUS_CONSTANT_ALPHA};function b(R,ue,z,Y,me,pe,We,Ct,Xt,mt){if(R===Gi){v===!0&&(Ie(n.BLEND),v=!1);return}if(v===!1&&(oe(n.BLEND),v=!0),R!==wM){if(R!==m||mt!==w){if((p!==vr||E!==vr)&&(n.blendEquation(n.FUNC_ADD),p=vr,E=vr),mt)switch(R){case rs:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case xg:n.blendFunc(n.ONE,n.ONE);break;case Eg:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Mg:n.blendFuncSeparate(n.ZERO,n.SRC_COLOR,n.ZERO,n.SRC_ALPHA);break;default:console.error("THREE.WebGLState: Invalid blending: ",R);break}else switch(R){case rs:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case xg:n.blendFunc(n.SRC_ALPHA,n.ONE);break;case Eg:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Mg:n.blendFunc(n.ZERO,n.SRC_COLOR);break;default:console.error("THREE.WebGLState: Invalid blending: ",R);break}T=null,M=null,I=null,D=null,C.set(0,0,0),N=0,m=R,w=mt}return}me=me||ue,pe=pe||z,We=We||Y,(ue!==p||me!==E)&&(n.blendEquationSeparate(ee[ue],ee[me]),p=ue,E=me),(z!==T||Y!==M||pe!==I||We!==D)&&(n.blendFuncSeparate(J[z],J[Y],J[pe],J[We]),T=z,M=Y,I=pe,D=We),(Ct.equals(C)===!1||Xt!==N)&&(n.blendColor(Ct.r,Ct.g,Ct.b,Xt),C.copy(Ct),N=Xt),m=R,w=!1}function Ce(R,ue){R.side===pi?Ie(n.CULL_FACE):oe(n.CULL_FACE);let z=R.side===sn;ue&&(z=!z),te(z),R.blending===rs&&R.transparent===!1?b(Gi):b(R.blending,R.blendEquation,R.blendSrc,R.blendDst,R.blendEquationAlpha,R.blendSrcAlpha,R.blendDstAlpha,R.blendColor,R.blendAlpha,R.premultipliedAlpha),o.setFunc(R.depthFunc),o.setTest(R.depthTest),o.setMask(R.depthWrite),s.setMask(R.colorWrite);let Y=R.stencilWrite;a.setTest(Y),Y&&(a.setMask(R.stencilWriteMask),a.setFunc(R.stencilFunc,R.stencilRef,R.stencilFuncMask),a.setOp(R.stencilFail,R.stencilZFail,R.stencilZPass)),Fe(R.polygonOffset,R.polygonOffsetFactor,R.polygonOffsetUnits),R.alphaToCoverage===!0?oe(n.SAMPLE_ALPHA_TO_COVERAGE):Ie(n.SAMPLE_ALPHA_TO_COVERAGE)}function te(R){x!==R&&(R?n.frontFace(n.CW):n.frontFace(n.CCW),x=R)}function ye(R){R!==EM?(oe(n.CULL_FACE),R!==A&&(R===yg?n.cullFace(n.BACK):R===MM?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):Ie(n.CULL_FACE),A=R}function re(R){R!==$&&(H&&n.lineWidth(R),$=R)}function Fe(R,ue,z){R?(oe(n.POLYGON_OFFSET_FILL),(B!==ue||q!==z)&&(n.polygonOffset(ue,z),B=ue,q=z)):Ie(n.POLYGON_OFFSET_FILL)}function he(R){R?oe(n.SCISSOR_TEST):Ie(n.SCISSOR_TEST)}function S(R){R===void 0&&(R=n.TEXTURE0+X-1),ae!==R&&(n.activeTexture(R),ae=R)}function y(R,ue,z){z===void 0&&(ae===null?z=n.TEXTURE0+X-1:z=ae);let Y=ge[z];Y===void 0&&(Y={type:void 0,texture:void 0},ge[z]=Y),(Y.type!==R||Y.texture!==ue)&&(ae!==z&&(n.activeTexture(z),ae=z),n.bindTexture(R,ue||we[R]),Y.type=R,Y.texture=ue)}function F(){let R=ge[ae];R!==void 0&&R.type!==void 0&&(n.bindTexture(R.type,null),R.type=void 0,R.texture=void 0)}function G(){try{n.compressedTexImage2D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function K(){try{n.compressedTexImage3D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function j(){try{n.texSubImage2D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function Te(){try{n.texSubImage3D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function le(){try{n.compressedTexSubImage2D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function ve(){try{n.compressedTexSubImage3D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function Qe(){try{n.texStorage2D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function ie(){try{n.texStorage3D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function xe(){try{n.texImage2D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function Le(){try{n.texImage3D.apply(n,arguments)}catch(R){console.error("THREE.WebGLState:",R)}}function Ue(R){dt.equals(R)===!1&&(n.scissor(R.x,R.y,R.z,R.w),dt.copy(R))}function Ee(R){W.equals(R)===!1&&(n.viewport(R.x,R.y,R.z,R.w),W.copy(R))}function it(R,ue){let z=l.get(ue);z===void 0&&(z=new WeakMap,l.set(ue,z));let Y=z.get(R);Y===void 0&&(Y=n.getUniformBlockIndex(ue,R.name),z.set(R,Y))}function Ye(R,ue){let Y=l.get(ue).get(R);c.get(ue)!==Y&&(n.uniformBlockBinding(ue,Y,R.__bindingPointIndex),c.set(ue,Y))}function Et(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),u={},ae=null,ge={},d={},f=new WeakMap,h=[],g=null,v=!1,m=null,p=null,T=null,M=null,E=null,I=null,D=null,C=new tt(0,0,0),N=0,w=!1,x=null,A=null,$=null,B=null,q=null,dt.set(0,0,n.canvas.width,n.canvas.height),W.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:oe,disable:Ie,bindFramebuffer:ot,drawBuffers:De,useProgram:ut,setBlending:b,setMaterial:Ce,setFlipSided:te,setCullFace:ye,setLineWidth:re,setPolygonOffset:Fe,setScissorTest:he,activeTexture:S,bindTexture:y,unbindTexture:F,compressedTexImage2D:G,compressedTexImage3D:K,texImage2D:xe,texImage3D:Le,updateUBOMapping:it,uniformBlockBinding:Ye,texStorage2D:Qe,texStorage3D:ie,texSubImage2D:j,texSubImage3D:Te,compressedTexSubImage2D:le,compressedTexSubImage3D:ve,scissor:Ue,viewport:Ee,reset:Et}}function r2(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new fe,u=new WeakMap,d,f=new WeakMap,h=!1;try{h=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function g(S,y){return h?new OffscreenCanvas(S,y):No("canvas")}function v(S,y,F){let G=1,K=he(S);if((K.width>F||K.height>F)&&(G=F/Math.max(K.width,K.height)),G<1)if(typeof HTMLImageElement<"u"&&S instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&S instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&S instanceof ImageBitmap||typeof VideoFrame<"u"&&S instanceof VideoFrame){let j=Math.floor(G*K.width),Te=Math.floor(G*K.height);d===void 0&&(d=g(j,Te));let le=y?g(j,Te):d;return le.width=j,le.height=Te,le.getContext("2d").drawImage(S,0,0,j,Te),console.warn("THREE.WebGLRenderer: Texture has been resized from ("+K.width+"x"+K.height+") to ("+j+"x"+Te+")."),le}else return"data"in S&&console.warn("THREE.WebGLRenderer: Image in DataTexture is too big ("+K.width+"x"+K.height+")."),S;return S}function m(S){return S.generateMipmaps}function p(S){n.generateMipmap(S)}function T(S){return S.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:S.isWebGL3DRenderTarget?n.TEXTURE_3D:S.isWebGLArrayRenderTarget||S.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function M(S,y,F,G,K=!1){if(S!==null){if(n[S]!==void 0)return n[S];console.warn("THREE.WebGLRenderer: Attempt to use non-existing WebGL internal format '"+S+"'")}let j=y;if(y===n.RED&&(F===n.FLOAT&&(j=n.R32F),F===n.HALF_FLOAT&&(j=n.R16F),F===n.UNSIGNED_BYTE&&(j=n.R8)),y===n.RED_INTEGER&&(F===n.UNSIGNED_BYTE&&(j=n.R8UI),F===n.UNSIGNED_SHORT&&(j=n.R16UI),F===n.UNSIGNED_INT&&(j=n.R32UI),F===n.BYTE&&(j=n.R8I),F===n.SHORT&&(j=n.R16I),F===n.INT&&(j=n.R32I)),y===n.RG&&(F===n.FLOAT&&(j=n.RG32F),F===n.HALF_FLOAT&&(j=n.RG16F),F===n.UNSIGNED_BYTE&&(j=n.RG8)),y===n.RG_INTEGER&&(F===n.UNSIGNED_BYTE&&(j=n.RG8UI),F===n.UNSIGNED_SHORT&&(j=n.RG16UI),F===n.UNSIGNED_INT&&(j=n.RG32UI),F===n.BYTE&&(j=n.RG8I),F===n.SHORT&&(j=n.RG16I),F===n.INT&&(j=n.RG32I)),y===n.RGB_INTEGER&&(F===n.UNSIGNED_BYTE&&(j=n.RGB8UI),F===n.UNSIGNED_SHORT&&(j=n.RGB16UI),F===n.UNSIGNED_INT&&(j=n.RGB32UI),F===n.BYTE&&(j=n.RGB8I),F===n.SHORT&&(j=n.RGB16I),F===n.INT&&(j=n.RGB32I)),y===n.RGBA_INTEGER&&(F===n.UNSIGNED_BYTE&&(j=n.RGBA8UI),F===n.UNSIGNED_SHORT&&(j=n.RGBA16UI),F===n.UNSIGNED_INT&&(j=n.RGBA32UI),F===n.BYTE&&(j=n.RGBA8I),F===n.SHORT&&(j=n.RGBA16I),F===n.INT&&(j=n.RGBA32I)),y===n.RGB&&F===n.UNSIGNED_INT_5_9_9_9_REV&&(j=n.RGB9_E5),y===n.RGBA){let Te=K?Xa:ct.getTransfer(G);F===n.FLOAT&&(j=n.RGBA32F),F===n.HALF_FLOAT&&(j=n.RGBA16F),F===n.UNSIGNED_BYTE&&(j=Te===_t?n.SRGB8_ALPHA8:n.RGBA8),F===n.UNSIGNED_SHORT_4_4_4_4&&(j=n.RGBA4),F===n.UNSIGNED_SHORT_5_5_5_1&&(j=n.RGB5_A1)}return(j===n.R16F||j===n.R32F||j===n.RG16F||j===n.RG32F||j===n.RGBA16F||j===n.RGBA32F)&&e.get("EXT_color_buffer_float"),j}function E(S,y){let F;return S?y===null||y===br||y===ms?F=n.DEPTH24_STENCIL8:y===gi?F=n.DEPTH32F_STENCIL8:y===Vo&&(F=n.DEPTH24_STENCIL8,console.warn("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):y===null||y===br||y===ms?F=n.DEPTH_COMPONENT24:y===gi?F=n.DEPTH_COMPONENT32F:y===Vo&&(F=n.DEPTH_COMPONENT16),F}function I(S,y){return m(S)===!0||S.isFramebufferTexture&&S.minFilter!==Vn&&S.minFilter!==Qn?Math.log2(Math.max(y.width,y.height))+1:S.mipmaps!==void 0&&S.mipmaps.length>0?S.mipmaps.length:S.isCompressedTexture&&Array.isArray(S.image)?y.mipmaps.length:1}function D(S){let y=S.target;y.removeEventListener("dispose",D),N(y),y.isVideoTexture&&u.delete(y)}function C(S){let y=S.target;y.removeEventListener("dispose",C),x(y)}function N(S){let y=i.get(S);if(y.__webglInit===void 0)return;let F=S.source,G=f.get(F);if(G){let K=G[y.__cacheKey];K.usedTimes--,K.usedTimes===0&&w(S),Object.keys(G).length===0&&f.delete(F)}i.remove(S)}function w(S){let y=i.get(S);n.deleteTexture(y.__webglTexture);let F=S.source,G=f.get(F);delete G[y.__cacheKey],o.memory.textures--}function x(S){let y=i.get(S);if(S.depthTexture&&(S.depthTexture.dispose(),i.remove(S.depthTexture)),S.isWebGLCubeRenderTarget)for(let G=0;G<6;G++){if(Array.isArray(y.__webglFramebuffer[G]))for(let K=0;K<y.__webglFramebuffer[G].length;K++)n.deleteFramebuffer(y.__webglFramebuffer[G][K]);else n.deleteFramebuffer(y.__webglFramebuffer[G]);y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer[G])}else{if(Array.isArray(y.__webglFramebuffer))for(let G=0;G<y.__webglFramebuffer.length;G++)n.deleteFramebuffer(y.__webglFramebuffer[G]);else n.deleteFramebuffer(y.__webglFramebuffer);if(y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer),y.__webglMultisampledFramebuffer&&n.deleteFramebuffer(y.__webglMultisampledFramebuffer),y.__webglColorRenderbuffer)for(let G=0;G<y.__webglColorRenderbuffer.length;G++)y.__webglColorRenderbuffer[G]&&n.deleteRenderbuffer(y.__webglColorRenderbuffer[G]);y.__webglDepthRenderbuffer&&n.deleteRenderbuffer(y.__webglDepthRenderbuffer)}let F=S.textures;for(let G=0,K=F.length;G<K;G++){let j=i.get(F[G]);j.__webglTexture&&(n.deleteTexture(j.__webglTexture),o.memory.textures--),i.remove(F[G])}i.remove(S)}let A=0;function $(){A=0}function B(){let S=A;return S>=r.maxTextures&&console.warn("THREE.WebGLTextures: Trying to use "+S+" texture units while this GPU supports only "+r.maxTextures),A+=1,S}function q(S){let y=[];return y.push(S.wrapS),y.push(S.wrapT),y.push(S.wrapR||0),y.push(S.magFilter),y.push(S.minFilter),y.push(S.anisotropy),y.push(S.internalFormat),y.push(S.format),y.push(S.type),y.push(S.generateMipmaps),y.push(S.premultiplyAlpha),y.push(S.flipY),y.push(S.unpackAlignment),y.push(S.colorSpace),y.join()}function X(S,y){let F=i.get(S);if(S.isVideoTexture&&re(S),S.isRenderTargetTexture===!1&&S.version>0&&F.__version!==S.version){let G=S.image;if(G===null)console.warn("THREE.WebGLRenderer: Texture marked for update but no image data found.");else if(G.complete===!1)console.warn("THREE.WebGLRenderer: Texture marked for update but image is incomplete");else{W(F,S,y);return}}t.bindTexture(n.TEXTURE_2D,F.__webglTexture,n.TEXTURE0+y)}function H(S,y){let F=i.get(S);if(S.version>0&&F.__version!==S.version){W(F,S,y);return}t.bindTexture(n.TEXTURE_2D_ARRAY,F.__webglTexture,n.TEXTURE0+y)}function Z(S,y){let F=i.get(S);if(S.version>0&&F.__version!==S.version){W(F,S,y);return}t.bindTexture(n.TEXTURE_3D,F.__webglTexture,n.TEXTURE0+y)}function V(S,y){let F=i.get(S);if(S.version>0&&F.__version!==S.version){se(F,S,y);return}t.bindTexture(n.TEXTURE_CUBE_MAP,F.__webglTexture,n.TEXTURE0+y)}let ae={[Qu]:n.REPEAT,[gr]:n.CLAMP_TO_EDGE,[ed]:n.MIRRORED_REPEAT},ge={[Vn]:n.NEAREST,[KM]:n.NEAREST_MIPMAP_NEAREST,[Mc]:n.NEAREST_MIPMAP_LINEAR,[Qn]:n.LINEAR,[Ud]:n.LINEAR_MIPMAP_NEAREST,[wr]:n.LINEAR_MIPMAP_LINEAR},Se={[tS]:n.NEVER,[aS]:n.ALWAYS,[nS]:n.LESS,[Og]:n.LEQUAL,[iS]:n.EQUAL,[oS]:n.GEQUAL,[rS]:n.GREATER,[sS]:n.NOTEQUAL};function Ge(S,y){if(y.type===gi&&e.has("OES_texture_float_linear")===!1&&(y.magFilter===Qn||y.magFilter===Ud||y.magFilter===Mc||y.magFilter===wr||y.minFilter===Qn||y.minFilter===Ud||y.minFilter===Mc||y.minFilter===wr)&&console.warn("THREE.WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(S,n.TEXTURE_WRAP_S,ae[y.wrapS]),n.texParameteri(S,n.TEXTURE_WRAP_T,ae[y.wrapT]),(S===n.TEXTURE_3D||S===n.TEXTURE_2D_ARRAY)&&n.texParameteri(S,n.TEXTURE_WRAP_R,ae[y.wrapR]),n.texParameteri(S,n.TEXTURE_MAG_FILTER,ge[y.magFilter]),n.texParameteri(S,n.TEXTURE_MIN_FILTER,ge[y.minFilter]),y.compareFunction&&(n.texParameteri(S,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(S,n.TEXTURE_COMPARE_FUNC,Se[y.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(y.magFilter===Vn||y.minFilter!==Mc&&y.minFilter!==wr||y.type===gi&&e.has("OES_texture_float_linear")===!1)return;if(y.anisotropy>1||i.get(y).__currentAnisotropy){let F=e.get("EXT_texture_filter_anisotropic");n.texParameterf(S,F.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(y.anisotropy,r.getMaxAnisotropy())),i.get(y).__currentAnisotropy=y.anisotropy}}}function dt(S,y){let F=!1;S.__webglInit===void 0&&(S.__webglInit=!0,y.addEventListener("dispose",D));let G=y.source,K=f.get(G);K===void 0&&(K={},f.set(G,K));let j=q(y);if(j!==S.__cacheKey){K[j]===void 0&&(K[j]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,F=!0),K[j].usedTimes++;let Te=K[S.__cacheKey];Te!==void 0&&(K[S.__cacheKey].usedTimes--,Te.usedTimes===0&&w(y)),S.__cacheKey=j,S.__webglTexture=K[j].texture}return F}function W(S,y,F){let G=n.TEXTURE_2D;(y.isDataArrayTexture||y.isCompressedArrayTexture)&&(G=n.TEXTURE_2D_ARRAY),y.isData3DTexture&&(G=n.TEXTURE_3D);let K=dt(S,y),j=y.source;t.bindTexture(G,S.__webglTexture,n.TEXTURE0+F);let Te=i.get(j);if(j.version!==Te.__version||K===!0){t.activeTexture(n.TEXTURE0+F);let le=ct.getPrimaries(ct.workingColorSpace),ve=y.colorSpace===Wi?null:ct.getPrimaries(y.colorSpace),Qe=y.colorSpace===Wi||le===ve?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Qe);let ie=v(y.image,!1,r.maxTextureSize);ie=Fe(y,ie);let xe=s.convert(y.format,y.colorSpace),Le=s.convert(y.type),Ue=M(y.internalFormat,xe,Le,y.colorSpace,y.isVideoTexture);Ge(G,y);let Ee,it=y.mipmaps,Ye=y.isVideoTexture!==!0,Et=Te.__version===void 0||K===!0,R=j.dataReady,ue=I(y,ie);if(y.isDepthTexture)Ue=E(y.format===os,y.type),Et&&(Ye?t.texStorage2D(n.TEXTURE_2D,1,Ue,ie.width,ie.height):t.texImage2D(n.TEXTURE_2D,0,Ue,ie.width,ie.height,0,xe,Le,null));else if(y.isDataTexture)if(it.length>0){Ye&&Et&&t.texStorage2D(n.TEXTURE_2D,ue,Ue,it[0].width,it[0].height);for(let z=0,Y=it.length;z<Y;z++)Ee=it[z],Ye?R&&t.texSubImage2D(n.TEXTURE_2D,z,0,0,Ee.width,Ee.height,xe,Le,Ee.data):t.texImage2D(n.TEXTURE_2D,z,Ue,Ee.width,Ee.height,0,xe,Le,Ee.data);y.generateMipmaps=!1}else Ye?(Et&&t.texStorage2D(n.TEXTURE_2D,ue,Ue,ie.width,ie.height),R&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,ie.width,ie.height,xe,Le,ie.data)):t.texImage2D(n.TEXTURE_2D,0,Ue,ie.width,ie.height,0,xe,Le,ie.data);else if(y.isCompressedTexture)if(y.isCompressedArrayTexture){Ye&&Et&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ue,Ue,it[0].width,it[0].height,ie.depth);for(let z=0,Y=it.length;z<Y;z++)if(Ee=it[z],y.format!==Hn)if(xe!==null)if(Ye){if(R)if(y.layerUpdates.size>0){let me=zg(Ee.width,Ee.height,y.format,y.type);for(let pe of y.layerUpdates){let We=Ee.data.subarray(pe*me/Ee.data.BYTES_PER_ELEMENT,(pe+1)*me/Ee.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,z,0,0,pe,Ee.width,Ee.height,1,xe,We)}y.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,z,0,0,0,Ee.width,Ee.height,ie.depth,xe,Ee.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,z,Ue,Ee.width,Ee.height,ie.depth,0,Ee.data,0,0);else console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else Ye?R&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,z,0,0,0,Ee.width,Ee.height,ie.depth,xe,Le,Ee.data):t.texImage3D(n.TEXTURE_2D_ARRAY,z,Ue,Ee.width,Ee.height,ie.depth,0,xe,Le,Ee.data)}else{Ye&&Et&&t.texStorage2D(n.TEXTURE_2D,ue,Ue,it[0].width,it[0].height);for(let z=0,Y=it.length;z<Y;z++)Ee=it[z],y.format!==Hn?xe!==null?Ye?R&&t.compressedTexSubImage2D(n.TEXTURE_2D,z,0,0,Ee.width,Ee.height,xe,Ee.data):t.compressedTexImage2D(n.TEXTURE_2D,z,Ue,Ee.width,Ee.height,0,Ee.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):Ye?R&&t.texSubImage2D(n.TEXTURE_2D,z,0,0,Ee.width,Ee.height,xe,Le,Ee.data):t.texImage2D(n.TEXTURE_2D,z,Ue,Ee.width,Ee.height,0,xe,Le,Ee.data)}else if(y.isDataArrayTexture)if(Ye){if(Et&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ue,Ue,ie.width,ie.height,ie.depth),R)if(y.layerUpdates.size>0){let z=zg(ie.width,ie.height,y.format,y.type);for(let Y of y.layerUpdates){let me=ie.data.subarray(Y*z/ie.data.BYTES_PER_ELEMENT,(Y+1)*z/ie.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,Y,ie.width,ie.height,1,xe,Le,me)}y.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,ie.width,ie.height,ie.depth,xe,Le,ie.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,Ue,ie.width,ie.height,ie.depth,0,xe,Le,ie.data);else if(y.isData3DTexture)Ye?(Et&&t.texStorage3D(n.TEXTURE_3D,ue,Ue,ie.width,ie.height,ie.depth),R&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,ie.width,ie.height,ie.depth,xe,Le,ie.data)):t.texImage3D(n.TEXTURE_3D,0,Ue,ie.width,ie.height,ie.depth,0,xe,Le,ie.data);else if(y.isFramebufferTexture){if(Et)if(Ye)t.texStorage2D(n.TEXTURE_2D,ue,Ue,ie.width,ie.height);else{let z=ie.width,Y=ie.height;for(let me=0;me<ue;me++)t.texImage2D(n.TEXTURE_2D,me,Ue,z,Y,0,xe,Le,null),z>>=1,Y>>=1}}else if(it.length>0){if(Ye&&Et){let z=he(it[0]);t.texStorage2D(n.TEXTURE_2D,ue,Ue,z.width,z.height)}for(let z=0,Y=it.length;z<Y;z++)Ee=it[z],Ye?R&&t.texSubImage2D(n.TEXTURE_2D,z,0,0,xe,Le,Ee):t.texImage2D(n.TEXTURE_2D,z,Ue,xe,Le,Ee);y.generateMipmaps=!1}else if(Ye){if(Et){let z=he(ie);t.texStorage2D(n.TEXTURE_2D,ue,Ue,z.width,z.height)}R&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,xe,Le,ie)}else t.texImage2D(n.TEXTURE_2D,0,Ue,xe,Le,ie);m(y)&&p(G),Te.__version=j.version,y.onUpdate&&y.onUpdate(y)}S.__version=y.version}function se(S,y,F){if(y.image.length!==6)return;let G=dt(S,y),K=y.source;t.bindTexture(n.TEXTURE_CUBE_MAP,S.__webglTexture,n.TEXTURE0+F);let j=i.get(K);if(K.version!==j.__version||G===!0){t.activeTexture(n.TEXTURE0+F);let Te=ct.getPrimaries(ct.workingColorSpace),le=y.colorSpace===Wi?null:ct.getPrimaries(y.colorSpace),ve=y.colorSpace===Wi||Te===le?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,ve);let Qe=y.isCompressedTexture||y.image[0].isCompressedTexture,ie=y.image[0]&&y.image[0].isDataTexture,xe=[];for(let Y=0;Y<6;Y++)!Qe&&!ie?xe[Y]=v(y.image[Y],!0,r.maxCubemapSize):xe[Y]=ie?y.image[Y].image:y.image[Y],xe[Y]=Fe(y,xe[Y]);let Le=xe[0],Ue=s.convert(y.format,y.colorSpace),Ee=s.convert(y.type),it=M(y.internalFormat,Ue,Ee,y.colorSpace),Ye=y.isVideoTexture!==!0,Et=j.__version===void 0||G===!0,R=K.dataReady,ue=I(y,Le);Ge(n.TEXTURE_CUBE_MAP,y);let z;if(Qe){Ye&&Et&&t.texStorage2D(n.TEXTURE_CUBE_MAP,ue,it,Le.width,Le.height);for(let Y=0;Y<6;Y++){z=xe[Y].mipmaps;for(let me=0;me<z.length;me++){let pe=z[me];y.format!==Hn?Ue!==null?Ye?R&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,me,0,0,pe.width,pe.height,Ue,pe.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,me,it,pe.width,pe.height,0,pe.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):Ye?R&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,me,0,0,pe.width,pe.height,Ue,Ee,pe.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,me,it,pe.width,pe.height,0,Ue,Ee,pe.data)}}}else{if(z=y.mipmaps,Ye&&Et){z.length>0&&ue++;let Y=he(xe[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,ue,it,Y.width,Y.height)}for(let Y=0;Y<6;Y++)if(ie){Ye?R&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,0,0,0,xe[Y].width,xe[Y].height,Ue,Ee,xe[Y].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,0,it,xe[Y].width,xe[Y].height,0,Ue,Ee,xe[Y].data);for(let me=0;me<z.length;me++){let We=z[me].image[Y].image;Ye?R&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,me+1,0,0,We.width,We.height,Ue,Ee,We.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,me+1,it,We.width,We.height,0,Ue,Ee,We.data)}}else{Ye?R&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,0,0,0,Ue,Ee,xe[Y]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,0,it,Ue,Ee,xe[Y]);for(let me=0;me<z.length;me++){let pe=z[me];Ye?R&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,me+1,0,0,Ue,Ee,pe.image[Y]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,me+1,it,Ue,Ee,pe.image[Y])}}}m(y)&&p(n.TEXTURE_CUBE_MAP),j.__version=K.version,y.onUpdate&&y.onUpdate(y)}S.__version=y.version}function we(S,y,F,G,K,j){let Te=s.convert(F.format,F.colorSpace),le=s.convert(F.type),ve=M(F.internalFormat,Te,le,F.colorSpace),Qe=i.get(y),ie=i.get(F);if(ie.__renderTarget=y,!Qe.__hasExternalTextures){let xe=Math.max(1,y.width>>j),Le=Math.max(1,y.height>>j);K===n.TEXTURE_3D||K===n.TEXTURE_2D_ARRAY?t.texImage3D(K,j,ve,xe,Le,y.depth,0,Te,le,null):t.texImage2D(K,j,ve,xe,Le,0,Te,le,null)}t.bindFramebuffer(n.FRAMEBUFFER,S),ye(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,G,K,ie.__webglTexture,0,te(y)):(K===n.TEXTURE_2D||K>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&K<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,G,K,ie.__webglTexture,j),t.bindFramebuffer(n.FRAMEBUFFER,null)}function oe(S,y,F){if(n.bindRenderbuffer(n.RENDERBUFFER,S),y.depthBuffer){let G=y.depthTexture,K=G&&G.isDepthTexture?G.type:null,j=E(y.stencilBuffer,K),Te=y.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,le=te(y);ye(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,le,j,y.width,y.height):F?n.renderbufferStorageMultisample(n.RENDERBUFFER,le,j,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,j,y.width,y.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,Te,n.RENDERBUFFER,S)}else{let G=y.textures;for(let K=0;K<G.length;K++){let j=G[K],Te=s.convert(j.format,j.colorSpace),le=s.convert(j.type),ve=M(j.internalFormat,Te,le,j.colorSpace),Qe=te(y);F&&ye(y)===!1?n.renderbufferStorageMultisample(n.RENDERBUFFER,Qe,ve,y.width,y.height):ye(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,Qe,ve,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,ve,y.width,y.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function Ie(S,y){if(y&&y.isWebGLCubeRenderTarget)throw new Error("Depth Texture with cube render targets is not supported");if(t.bindFramebuffer(n.FRAMEBUFFER,S),!(y.depthTexture&&y.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let G=i.get(y.depthTexture);G.__renderTarget=y,(!G.__webglTexture||y.depthTexture.image.width!==y.width||y.depthTexture.image.height!==y.height)&&(y.depthTexture.image.width=y.width,y.depthTexture.image.height=y.height,y.depthTexture.needsUpdate=!0),X(y.depthTexture,0);let K=G.__webglTexture,j=te(y);if(y.depthTexture.format===ts)ye(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,K,0,j):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,K,0);else if(y.depthTexture.format===os)ye(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,K,0,j):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,K,0);else throw new Error("Unknown depthTexture format")}function ot(S){let y=i.get(S),F=S.isWebGLCubeRenderTarget===!0;if(y.__boundDepthTexture!==S.depthTexture){let G=S.depthTexture;if(y.__depthDisposeCallback&&y.__depthDisposeCallback(),G){let K=()=>{delete y.__boundDepthTexture,delete y.__depthDisposeCallback,G.removeEventListener("dispose",K)};G.addEventListener("dispose",K),y.__depthDisposeCallback=K}y.__boundDepthTexture=G}if(S.depthTexture&&!y.__autoAllocateDepthBuffer){if(F)throw new Error("target.depthTexture not supported in Cube render targets");Ie(y.__webglFramebuffer,S)}else if(F){y.__webglDepthbuffer=[];for(let G=0;G<6;G++)if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer[G]),y.__webglDepthbuffer[G]===void 0)y.__webglDepthbuffer[G]=n.createRenderbuffer(),oe(y.__webglDepthbuffer[G],S,!1);else{let K=S.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,j=y.__webglDepthbuffer[G];n.bindRenderbuffer(n.RENDERBUFFER,j),n.framebufferRenderbuffer(n.FRAMEBUFFER,K,n.RENDERBUFFER,j)}}else if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer),y.__webglDepthbuffer===void 0)y.__webglDepthbuffer=n.createRenderbuffer(),oe(y.__webglDepthbuffer,S,!1);else{let G=S.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,K=y.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,K),n.framebufferRenderbuffer(n.FRAMEBUFFER,G,n.RENDERBUFFER,K)}t.bindFramebuffer(n.FRAMEBUFFER,null)}function De(S,y,F){let G=i.get(S);y!==void 0&&we(G.__webglFramebuffer,S,S.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),F!==void 0&&ot(S)}function ut(S){let y=S.texture,F=i.get(S),G=i.get(y);S.addEventListener("dispose",C);let K=S.textures,j=S.isWebGLCubeRenderTarget===!0,Te=K.length>1;if(Te||(G.__webglTexture===void 0&&(G.__webglTexture=n.createTexture()),G.__version=y.version,o.memory.textures++),j){F.__webglFramebuffer=[];for(let le=0;le<6;le++)if(y.mipmaps&&y.mipmaps.length>0){F.__webglFramebuffer[le]=[];for(let ve=0;ve<y.mipmaps.length;ve++)F.__webglFramebuffer[le][ve]=n.createFramebuffer()}else F.__webglFramebuffer[le]=n.createFramebuffer()}else{if(y.mipmaps&&y.mipmaps.length>0){F.__webglFramebuffer=[];for(let le=0;le<y.mipmaps.length;le++)F.__webglFramebuffer[le]=n.createFramebuffer()}else F.__webglFramebuffer=n.createFramebuffer();if(Te)for(let le=0,ve=K.length;le<ve;le++){let Qe=i.get(K[le]);Qe.__webglTexture===void 0&&(Qe.__webglTexture=n.createTexture(),o.memory.textures++)}if(S.samples>0&&ye(S)===!1){F.__webglMultisampledFramebuffer=n.createFramebuffer(),F.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,F.__webglMultisampledFramebuffer);for(let le=0;le<K.length;le++){let ve=K[le];F.__webglColorRenderbuffer[le]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,F.__webglColorRenderbuffer[le]);let Qe=s.convert(ve.format,ve.colorSpace),ie=s.convert(ve.type),xe=M(ve.internalFormat,Qe,ie,ve.colorSpace,S.isXRRenderTarget===!0),Le=te(S);n.renderbufferStorageMultisample(n.RENDERBUFFER,Le,xe,S.width,S.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+le,n.RENDERBUFFER,F.__webglColorRenderbuffer[le])}n.bindRenderbuffer(n.RENDERBUFFER,null),S.depthBuffer&&(F.__webglDepthRenderbuffer=n.createRenderbuffer(),oe(F.__webglDepthRenderbuffer,S,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(j){t.bindTexture(n.TEXTURE_CUBE_MAP,G.__webglTexture),Ge(n.TEXTURE_CUBE_MAP,y);for(let le=0;le<6;le++)if(y.mipmaps&&y.mipmaps.length>0)for(let ve=0;ve<y.mipmaps.length;ve++)we(F.__webglFramebuffer[le][ve],S,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+le,ve);else we(F.__webglFramebuffer[le],S,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+le,0);m(y)&&p(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(Te){for(let le=0,ve=K.length;le<ve;le++){let Qe=K[le],ie=i.get(Qe);t.bindTexture(n.TEXTURE_2D,ie.__webglTexture),Ge(n.TEXTURE_2D,Qe),we(F.__webglFramebuffer,S,Qe,n.COLOR_ATTACHMENT0+le,n.TEXTURE_2D,0),m(Qe)&&p(n.TEXTURE_2D)}t.unbindTexture()}else{let le=n.TEXTURE_2D;if((S.isWebGL3DRenderTarget||S.isWebGLArrayRenderTarget)&&(le=S.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(le,G.__webglTexture),Ge(le,y),y.mipmaps&&y.mipmaps.length>0)for(let ve=0;ve<y.mipmaps.length;ve++)we(F.__webglFramebuffer[ve],S,y,n.COLOR_ATTACHMENT0,le,ve);else we(F.__webglFramebuffer,S,y,n.COLOR_ATTACHMENT0,le,0);m(y)&&p(le),t.unbindTexture()}S.depthBuffer&&ot(S)}function ee(S){let y=S.textures;for(let F=0,G=y.length;F<G;F++){let K=y[F];if(m(K)){let j=T(S),Te=i.get(K).__webglTexture;t.bindTexture(j,Te),p(j),t.unbindTexture()}}}let J=[],b=[];function Ce(S){if(S.samples>0){if(ye(S)===!1){let y=S.textures,F=S.width,G=S.height,K=n.COLOR_BUFFER_BIT,j=S.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,Te=i.get(S),le=y.length>1;if(le)for(let ve=0;ve<y.length;ve++)t.bindFramebuffer(n.FRAMEBUFFER,Te.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+ve,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,Te.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+ve,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,Te.__webglMultisampledFramebuffer),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Te.__webglFramebuffer);for(let ve=0;ve<y.length;ve++){if(S.resolveDepthBuffer&&(S.depthBuffer&&(K|=n.DEPTH_BUFFER_BIT),S.stencilBuffer&&S.resolveStencilBuffer&&(K|=n.STENCIL_BUFFER_BIT)),le){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,Te.__webglColorRenderbuffer[ve]);let Qe=i.get(y[ve]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,Qe,0)}n.blitFramebuffer(0,0,F,G,0,0,F,G,K,n.NEAREST),c===!0&&(J.length=0,b.length=0,J.push(n.COLOR_ATTACHMENT0+ve),S.depthBuffer&&S.resolveDepthBuffer===!1&&(J.push(j),b.push(j),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,b)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,J))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),le)for(let ve=0;ve<y.length;ve++){t.bindFramebuffer(n.FRAMEBUFFER,Te.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+ve,n.RENDERBUFFER,Te.__webglColorRenderbuffer[ve]);let Qe=i.get(y[ve]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,Te.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+ve,n.TEXTURE_2D,Qe,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Te.__webglMultisampledFramebuffer)}else if(S.depthBuffer&&S.resolveDepthBuffer===!1&&c){let y=S.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[y])}}}function te(S){return Math.min(r.maxSamples,S.samples)}function ye(S){let y=i.get(S);return S.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&y.__useRenderToTexture!==!1}function re(S){let y=o.render.frame;u.get(S)!==y&&(u.set(S,y),S.update())}function Fe(S,y){let F=S.colorSpace,G=S.format,K=S.type;return S.isCompressedTexture===!0||S.isVideoTexture===!0||F!==as&&F!==Wi&&(ct.getTransfer(F)===_t?(G!==Hn||K!==mi)&&console.warn("THREE.WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):console.error("THREE.WebGLTextures: Unsupported texture color space:",F)),y}function he(S){return typeof HTMLImageElement<"u"&&S instanceof HTMLImageElement?(l.width=S.naturalWidth||S.width,l.height=S.naturalHeight||S.height):typeof VideoFrame<"u"&&S instanceof VideoFrame?(l.width=S.displayWidth,l.height=S.displayHeight):(l.width=S.width,l.height=S.height),l}this.allocateTextureUnit=B,this.resetTextureUnits=$,this.setTexture2D=X,this.setTexture2DArray=H,this.setTexture3D=Z,this.setTextureCube=V,this.rebindTextures=De,this.setupRenderTarget=ut,this.updateRenderTargetMipmap=ee,this.updateMultisampleRenderTarget=Ce,this.setupDepthRenderbuffer=ot,this.setupFrameBufferTexture=we,this.useMultisampledRTT=ye}function s2(n,e){function t(i,r=Wi){let s,o=ct.getTransfer(r);if(i===mi)return n.UNSIGNED_BYTE;if(i===Vd)return n.UNSIGNED_SHORT_4_4_4_4;if(i===zd)return n.UNSIGNED_SHORT_5_5_5_1;if(i===bg)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===Sg)return n.BYTE;if(i===wg)return n.SHORT;if(i===Vo)return n.UNSIGNED_SHORT;if(i===Bd)return n.INT;if(i===br)return n.UNSIGNED_INT;if(i===gi)return n.FLOAT;if(i===zo)return n.HALF_FLOAT;if(i===Tg)return n.ALPHA;if(i===Cg)return n.RGB;if(i===Hn)return n.RGBA;if(i===Dg)return n.LUMINANCE;if(i===Ag)return n.LUMINANCE_ALPHA;if(i===ts)return n.DEPTH_COMPONENT;if(i===os)return n.DEPTH_STENCIL;if(i===Ig)return n.RED;if(i===Hd)return n.RED_INTEGER;if(i===Rg)return n.RG;if(i===Gd)return n.RG_INTEGER;if(i===jd)return n.RGBA_INTEGER;if(i===Sc||i===wc||i===bc||i===Tc)if(o===_t)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===Sc)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===wc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===bc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===Tc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===Sc)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===wc)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===bc)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===Tc)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===Wd||i===$d||i===qd||i===Xd)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===Wd)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===$d)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===qd)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===Xd)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===Yd||i===Zd||i===Kd)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===Yd||i===Zd)return o===_t?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===Kd)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC}else return null;if(i===Jd||i===Qd||i===ef||i===tf||i===nf||i===rf||i===sf||i===of||i===af||i===cf||i===lf||i===uf||i===df||i===ff)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===Jd)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===Qd)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===ef)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===tf)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===nf)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===rf)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===sf)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===of)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===af)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===cf)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===lf)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===uf)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===df)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===ff)return o===_t?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Cc||i===hf||i===pf)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Cc)return o===_t?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===hf)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===pf)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===Ng||i===mf||i===gf||i===vf)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Cc)return s.COMPRESSED_RED_RGTC1_EXT;if(i===mf)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===gf)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===vf)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===ms?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var o2=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,a2=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,ev=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t,i){if(this.texture===null){let r=new $i,s=e.properties.get(r);s.__webglTexture=t.texture,(t.depthNear!==i.depthNear||t.depthFar!==i.depthFar)&&(this.depthNear=t.depthNear,this.depthFar=t.depthFar),this.texture=r}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new ei({vertexShader:o2,fragmentShader:a2,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new Gt(new ls(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},tv=class extends zi{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,d=null,f=null,h=null,g=null,v=new ev,m=t.getContextAttributes(),p=null,T=null,M=[],E=[],I=new fe,D=null,C=new Qt;C.viewport=new Tt;let N=new Qt;N.viewport=new Tt;let w=[C,N],x=new Td,A=null,$=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(W){let se=M[W];return se===void 0&&(se=new Lo,M[W]=se),se.getTargetRaySpace()},this.getControllerGrip=function(W){let se=M[W];return se===void 0&&(se=new Lo,M[W]=se),se.getGripSpace()},this.getHand=function(W){let se=M[W];return se===void 0&&(se=new Lo,M[W]=se),se.getHandSpace()};function B(W){let se=E.indexOf(W.inputSource);if(se===-1)return;let we=M[se];we!==void 0&&(we.update(W.inputSource,W.frame,l||o),we.dispatchEvent({type:W.type,data:W.inputSource}))}function q(){r.removeEventListener("select",B),r.removeEventListener("selectstart",B),r.removeEventListener("selectend",B),r.removeEventListener("squeeze",B),r.removeEventListener("squeezestart",B),r.removeEventListener("squeezeend",B),r.removeEventListener("end",q),r.removeEventListener("inputsourceschange",X);for(let W=0;W<M.length;W++){let se=E[W];se!==null&&(E[W]=null,M[W].disconnect(se))}A=null,$=null,v.reset(),e.setRenderTarget(p),h=null,f=null,d=null,r=null,T=null,dt.stop(),i.isPresenting=!1,e.setPixelRatio(D),e.setSize(I.width,I.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(W){s=W,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(W){a=W,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function(W){l=W},this.getBaseLayer=function(){return f!==null?f:h},this.getBinding=function(){return d},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=function(W){return _i(this,null,function*(){if(r=W,r!==null){if(p=e.getRenderTarget(),r.addEventListener("select",B),r.addEventListener("selectstart",B),r.addEventListener("selectend",B),r.addEventListener("squeeze",B),r.addEventListener("squeezestart",B),r.addEventListener("squeezeend",B),r.addEventListener("end",q),r.addEventListener("inputsourceschange",X),m.xrCompatible!==!0&&(yield t.makeXRCompatible()),D=e.getPixelRatio(),e.getSize(I),typeof XRWebGLBinding<"u"&&"createProjectionLayer"in XRWebGLBinding.prototype){let we=null,oe=null,Ie=null;m.depth&&(Ie=m.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,we=m.stencil?os:ts,oe=m.stencil?ms:br);let ot={colorFormat:t.RGBA8,depthFormat:Ie,scaleFactor:s};d=new XRWebGLBinding(r,t),f=d.createProjectionLayer(ot),r.updateRenderState({layers:[f]}),e.setPixelRatio(1),e.setSize(f.textureWidth,f.textureHeight,!1),T=new ui(f.textureWidth,f.textureHeight,{format:Hn,type:mi,depthTexture:new rc(f.textureWidth,f.textureHeight,oe,void 0,void 0,void 0,void 0,void 0,void 0,we),stencilBuffer:m.stencil,colorSpace:e.outputColorSpace,samples:m.antialias?4:0,resolveDepthBuffer:f.ignoreDepthValues===!1})}else{let we={antialias:m.antialias,alpha:!0,depth:m.depth,stencil:m.stencil,framebufferScaleFactor:s};h=new XRWebGLLayer(r,t,we),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),T=new ui(h.framebufferWidth,h.framebufferHeight,{format:Hn,type:mi,colorSpace:e.outputColorSpace,stencilBuffer:m.stencil})}T.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=yield r.requestReferenceSpace(a),dt.setContext(r),dt.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}})},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return v.getDepthTexture()};function X(W){for(let se=0;se<W.removed.length;se++){let we=W.removed[se],oe=E.indexOf(we);oe>=0&&(E[oe]=null,M[oe].disconnect(we))}for(let se=0;se<W.added.length;se++){let we=W.added[se],oe=E.indexOf(we);if(oe===-1){for(let ot=0;ot<M.length;ot++)if(ot>=E.length){E.push(we),oe=ot;break}else if(E[ot]===null){E[ot]=we,oe=ot;break}if(oe===-1)break}let Ie=M[oe];Ie&&Ie.connect(we)}}let H=new L,Z=new L;function V(W,se,we){H.setFromMatrixPosition(se.matrixWorld),Z.setFromMatrixPosition(we.matrixWorld);let oe=H.distanceTo(Z),Ie=se.projectionMatrix.elements,ot=we.projectionMatrix.elements,De=Ie[14]/(Ie[10]-1),ut=Ie[14]/(Ie[10]+1),ee=(Ie[9]+1)/Ie[5],J=(Ie[9]-1)/Ie[5],b=(Ie[8]-1)/Ie[0],Ce=(ot[8]+1)/ot[0],te=De*b,ye=De*Ce,re=oe/(-b+Ce),Fe=re*-b;if(se.matrixWorld.decompose(W.position,W.quaternion,W.scale),W.translateX(Fe),W.translateZ(re),W.matrixWorld.compose(W.position,W.quaternion,W.scale),W.matrixWorldInverse.copy(W.matrixWorld).invert(),Ie[10]===-1)W.projectionMatrix.copy(se.projectionMatrix),W.projectionMatrixInverse.copy(se.projectionMatrixInverse);else{let he=De+re,S=ut+re,y=te-Fe,F=ye+(oe-Fe),G=ee*ut/S*he,K=J*ut/S*he;W.projectionMatrix.makePerspective(y,F,G,K,he,S),W.projectionMatrixInverse.copy(W.projectionMatrix).invert()}}function ae(W,se){se===null?W.matrixWorld.copy(W.matrix):W.matrixWorld.multiplyMatrices(se.matrixWorld,W.matrix),W.matrixWorldInverse.copy(W.matrixWorld).invert()}this.updateCamera=function(W){if(r===null)return;let se=W.near,we=W.far;v.texture!==null&&(v.depthNear>0&&(se=v.depthNear),v.depthFar>0&&(we=v.depthFar)),x.near=N.near=C.near=se,x.far=N.far=C.far=we,(A!==x.near||$!==x.far)&&(r.updateRenderState({depthNear:x.near,depthFar:x.far}),A=x.near,$=x.far),C.layers.mask=W.layers.mask|2,N.layers.mask=W.layers.mask|4,x.layers.mask=C.layers.mask|N.layers.mask;let oe=W.parent,Ie=x.cameras;ae(x,oe);for(let ot=0;ot<Ie.length;ot++)ae(Ie[ot],oe);Ie.length===2?V(x,C,N):x.projectionMatrix.copy(C.projectionMatrix),ge(W,x,oe)};function ge(W,se,we){we===null?W.matrix.copy(se.matrixWorld):(W.matrix.copy(we.matrixWorld),W.matrix.invert(),W.matrix.multiply(se.matrixWorld)),W.matrix.decompose(W.position,W.quaternion,W.scale),W.updateMatrixWorld(!0),W.projectionMatrix.copy(se.projectionMatrix),W.projectionMatrixInverse.copy(se.projectionMatrixInverse),W.isPerspectiveCamera&&(W.fov=nd*2*Math.atan(1/W.projectionMatrix.elements[5]),W.zoom=1)}this.getCamera=function(){return x},this.getFoveation=function(){if(!(f===null&&h===null))return c},this.setFoveation=function(W){c=W,f!==null&&(f.fixedFoveation=W),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=W)},this.hasDepthSensing=function(){return v.texture!==null},this.getDepthSensingMesh=function(){return v.getMesh(x)};let Se=null;function Ge(W,se){if(u=se.getViewerPose(l||o),g=se,u!==null){let we=u.views;h!==null&&(e.setRenderTargetFramebuffer(T,h.framebuffer),e.setRenderTarget(T));let oe=!1;we.length!==x.cameras.length&&(x.cameras.length=0,oe=!0);for(let De=0;De<we.length;De++){let ut=we[De],ee=null;if(h!==null)ee=h.getViewport(ut);else{let b=d.getViewSubImage(f,ut);ee=b.viewport,De===0&&(e.setRenderTargetTextures(T,b.colorTexture,f.ignoreDepthValues?void 0:b.depthStencilTexture),e.setRenderTarget(T))}let J=w[De];J===void 0&&(J=new Qt,J.layers.enable(De),J.viewport=new Tt,w[De]=J),J.matrix.fromArray(ut.transform.matrix),J.matrix.decompose(J.position,J.quaternion,J.scale),J.projectionMatrix.fromArray(ut.projectionMatrix),J.projectionMatrixInverse.copy(J.projectionMatrix).invert(),J.viewport.set(ee.x,ee.y,ee.width,ee.height),De===0&&(x.matrix.copy(J.matrix),x.matrix.decompose(x.position,x.quaternion,x.scale)),oe===!0&&x.cameras.push(J)}let Ie=r.enabledFeatures;if(Ie&&Ie.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&d){let De=d.getDepthInformation(we[0]);De&&De.isValid&&De.texture&&v.init(e,De,r.renderState)}}for(let we=0;we<M.length;we++){let oe=E[we],Ie=M[we];oe!==null&&Ie!==void 0&&Ie.update(oe,se,l||o)}Se&&Se(W,se),se.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:se}),g=null}let dt=new zS;dt.setAnimationLoop(Ge),this.setAnimationLoop=function(W){Se=W},this.dispose=function(){}}},_s=new _r,c2=new bt;function l2(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,kg(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,T,M,E){p.isMeshBasicMaterial||p.isMeshLambertMaterial?s(m,p):p.isMeshToonMaterial?(s(m,p),d(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p)):p.isMeshStandardMaterial?(s(m,p),f(m,p),p.isMeshPhysicalMaterial&&h(m,p,E)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),v(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?c(m,p,T,M):p.isSpriteMaterial?l(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===sn&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===sn&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let T=e.get(p),M=T.envMap,E=T.envMapRotation;M&&(m.envMap.value=M,_s.copy(E),_s.x*=-1,_s.y*=-1,_s.z*=-1,M.isCubeTexture&&M.isRenderTargetTexture===!1&&(_s.y*=-1,_s.z*=-1),m.envMapRotation.value.setFromMatrix4(c2.makeRotationFromEuler(_s)),m.flipEnvMap.value=M.isCubeTexture&&M.isRenderTargetTexture===!1?-1:1,m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function c(m,p,T,M){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*T,m.scale.value=M*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function l(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function d(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function f(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function h(m,p,T){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===sn&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=T.texture,m.transmissionSamplerSize.value.set(T.width,T.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function v(m,p){let T=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(T.matrixWorld),m.nearDistance.value=T.shadow.camera.near,m.farDistance.value=T.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function u2(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(T,M){let E=M.program;i.uniformBlockBinding(T,E)}function l(T,M){let E=r[T.id];E===void 0&&(g(T),E=u(T),r[T.id]=E,T.addEventListener("dispose",m));let I=M.program;i.updateUBOMapping(T,I);let D=e.render.frame;s[T.id]!==D&&(f(T),s[T.id]=D)}function u(T){let M=d();T.__bindingPointIndex=M;let E=n.createBuffer(),I=T.__size,D=T.usage;return n.bindBuffer(n.UNIFORM_BUFFER,E),n.bufferData(n.UNIFORM_BUFFER,I,D),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,M,E),E}function d(){for(let T=0;T<a;T++)if(o.indexOf(T)===-1)return o.push(T),T;return console.error("THREE.WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function f(T){let M=r[T.id],E=T.uniforms,I=T.__cache;n.bindBuffer(n.UNIFORM_BUFFER,M);for(let D=0,C=E.length;D<C;D++){let N=Array.isArray(E[D])?E[D]:[E[D]];for(let w=0,x=N.length;w<x;w++){let A=N[w];if(h(A,D,w,I)===!0){let $=A.__offset,B=Array.isArray(A.value)?A.value:[A.value],q=0;for(let X=0;X<B.length;X++){let H=B[X],Z=v(H);typeof H=="number"||typeof H=="boolean"?(A.__data[0]=H,n.bufferSubData(n.UNIFORM_BUFFER,$+q,A.__data)):H.isMatrix3?(A.__data[0]=H.elements[0],A.__data[1]=H.elements[1],A.__data[2]=H.elements[2],A.__data[3]=0,A.__data[4]=H.elements[3],A.__data[5]=H.elements[4],A.__data[6]=H.elements[5],A.__data[7]=0,A.__data[8]=H.elements[6],A.__data[9]=H.elements[7],A.__data[10]=H.elements[8],A.__data[11]=0):(H.toArray(A.__data,q),q+=Z.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,$,A.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(T,M,E,I){let D=T.value,C=M+"_"+E;if(I[C]===void 0)return typeof D=="number"||typeof D=="boolean"?I[C]=D:I[C]=D.clone(),!0;{let N=I[C];if(typeof D=="number"||typeof D=="boolean"){if(N!==D)return I[C]=D,!0}else if(N.equals(D)===!1)return N.copy(D),!0}return!1}function g(T){let M=T.uniforms,E=0,I=16;for(let C=0,N=M.length;C<N;C++){let w=Array.isArray(M[C])?M[C]:[M[C]];for(let x=0,A=w.length;x<A;x++){let $=w[x],B=Array.isArray($.value)?$.value:[$.value];for(let q=0,X=B.length;q<X;q++){let H=B[q],Z=v(H),V=E%I,ae=V%Z.boundary,ge=V+ae;E+=ae,ge!==0&&I-ge<Z.storage&&(E+=I-ge),$.__data=new Float32Array(Z.storage/Float32Array.BYTES_PER_ELEMENT),$.__offset=E,E+=Z.storage}}}let D=E%I;return D>0&&(E+=I-D),T.__size=E,T.__cache={},this}function v(T){let M={boundary:0,storage:0};return typeof T=="number"||typeof T=="boolean"?(M.boundary=4,M.storage=4):T.isVector2?(M.boundary=8,M.storage=8):T.isVector3||T.isColor?(M.boundary=16,M.storage=12):T.isVector4?(M.boundary=16,M.storage=16):T.isMatrix3?(M.boundary=48,M.storage=48):T.isMatrix4?(M.boundary=64,M.storage=64):T.isTexture?console.warn("THREE.WebGLRenderer: Texture samplers can not be part of an uniforms group."):console.warn("THREE.WebGLRenderer: Unsupported uniform value type.",T),M}function m(T){let M=T.target;M.removeEventListener("dispose",m);let E=o.indexOf(M.__bindingPointIndex);o.splice(E,1),n.deleteBuffer(r[M.id]),delete r[M.id],delete s[M.id]}function p(){for(let T in r)n.deleteBuffer(r[T]);o=[],r={},s={}}return{bind:c,update:l,dispose:p}}var Sf=class{constructor(e={}){let{canvas:t=cS(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1,reverseDepthBuffer:f=!1}=e;this.isWebGLRenderer=!0;let h;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");h=i.getContextAttributes().alpha}else h=o;let g=new Uint32Array(4),v=new Int32Array(4),m=null,p=null,T=[],M=[];this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this._outputColorSpace=Tn,this.toneMapping=ji,this.toneMappingExposure=1;let E=this,I=!1,D=0,C=0,N=null,w=-1,x=null,A=new Tt,$=new Tt,B=null,q=new tt(0),X=0,H=t.width,Z=t.height,V=1,ae=null,ge=null,Se=new Tt(0,0,H,Z),Ge=new Tt(0,0,H,Z),dt=!1,W=new Fo,se=!1,we=!1;this.transmissionResolutionScale=1;let oe=new bt,Ie=new bt,ot=new L,De=new Tt,ut={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},ee=!1;function J(){return N===null?V:1}let b=i;function Ce(_,P){return t.getContext(_,P)}try{let _={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${Cd}`),t.addEventListener("webglcontextlost",Y,!1),t.addEventListener("webglcontextrestored",me,!1),t.addEventListener("webglcontextcreationerror",pe,!1),b===null){let P="webgl2";if(b=Ce(P,_),b===null)throw Ce(P)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(_){throw console.error("THREE.WebGLRenderer: "+_.message),_}let te,ye,re,Fe,he,S,y,F,G,K,j,Te,le,ve,Qe,ie,xe,Le,Ue,Ee,it,Ye,Et,R;function ue(){te=new DF(b),te.init(),Ye=new s2(b,te),ye=new MF(b,te,e,Ye),re=new i2(b,te),ye.reverseDepthBuffer&&f&&re.buffers.depth.setReversed(!0),Fe=new RF(b),he=new jk,S=new r2(b,te,re,he,ye,Ye,Fe),y=new wF(E),F=new CF(E),G=new UP(b),Et=new xF(b,G),K=new AF(b,G,Fe,Et),j=new PF(b,K,G,Fe),Ue=new NF(b,ye,S),ie=new SF(he),Te=new Gk(E,y,F,te,ye,Et,ie),le=new l2(E,he),ve=new $k,Qe=new Jk(te),Le=new _F(E,y,F,re,j,h,c),xe=new t2(E,j,ye),R=new u2(b,Fe,ye,re),Ee=new EF(b,te,Fe),it=new IF(b,te,Fe),Fe.programs=Te.programs,E.capabilities=ye,E.extensions=te,E.properties=he,E.renderLists=ve,E.shadowMap=xe,E.state=re,E.info=Fe}ue();let z=new tv(E,b);this.xr=z,this.getContext=function(){return b},this.getContextAttributes=function(){return b.getContextAttributes()},this.forceContextLoss=function(){let _=te.get("WEBGL_lose_context");_&&_.loseContext()},this.forceContextRestore=function(){let _=te.get("WEBGL_lose_context");_&&_.restoreContext()},this.getPixelRatio=function(){return V},this.setPixelRatio=function(_){_!==void 0&&(V=_,this.setSize(H,Z,!1))},this.getSize=function(_){return _.set(H,Z)},this.setSize=function(_,P,k=!0){if(z.isPresenting){console.warn("THREE.WebGLRenderer: Can't change size while VR device is presenting.");return}H=_,Z=P,t.width=Math.floor(_*V),t.height=Math.floor(P*V),k===!0&&(t.style.width=_+"px",t.style.height=P+"px"),this.setViewport(0,0,_,P)},this.getDrawingBufferSize=function(_){return _.set(H*V,Z*V).floor()},this.setDrawingBufferSize=function(_,P,k){H=_,Z=P,V=k,t.width=Math.floor(_*k),t.height=Math.floor(P*k),this.setViewport(0,0,_,P)},this.getCurrentViewport=function(_){return _.copy(A)},this.getViewport=function(_){return _.copy(Se)},this.setViewport=function(_,P,k,U){_.isVector4?Se.set(_.x,_.y,_.z,_.w):Se.set(_,P,k,U),re.viewport(A.copy(Se).multiplyScalar(V).round())},this.getScissor=function(_){return _.copy(Ge)},this.setScissor=function(_,P,k,U){_.isVector4?Ge.set(_.x,_.y,_.z,_.w):Ge.set(_,P,k,U),re.scissor($.copy(Ge).multiplyScalar(V).round())},this.getScissorTest=function(){return dt},this.setScissorTest=function(_){re.setScissorTest(dt=_)},this.setOpaqueSort=function(_){ae=_},this.setTransparentSort=function(_){ge=_},this.getClearColor=function(_){return _.copy(Le.getClearColor())},this.setClearColor=function(){Le.setClearColor.apply(Le,arguments)},this.getClearAlpha=function(){return Le.getClearAlpha()},this.setClearAlpha=function(){Le.setClearAlpha.apply(Le,arguments)},this.clear=function(_=!0,P=!0,k=!0){let U=0;if(_){let O=!1;if(N!==null){let ne=N.texture.format;O=ne===jd||ne===Gd||ne===Hd}if(O){let ne=N.texture.type,de=ne===mi||ne===br||ne===Vo||ne===ms||ne===Vd||ne===zd,_e=Le.getClearColor(),Me=Le.getClearAlpha(),Be=_e.r,Ve=_e.g,Re=_e.b;de?(g[0]=Be,g[1]=Ve,g[2]=Re,g[3]=Me,b.clearBufferuiv(b.COLOR,0,g)):(v[0]=Be,v[1]=Ve,v[2]=Re,v[3]=Me,b.clearBufferiv(b.COLOR,0,v))}else U|=b.COLOR_BUFFER_BIT}P&&(U|=b.DEPTH_BUFFER_BIT),k&&(U|=b.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),b.clear(U)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.dispose=function(){t.removeEventListener("webglcontextlost",Y,!1),t.removeEventListener("webglcontextrestored",me,!1),t.removeEventListener("webglcontextcreationerror",pe,!1),Le.dispose(),ve.dispose(),Qe.dispose(),he.dispose(),y.dispose(),F.dispose(),j.dispose(),Et.dispose(),R.dispose(),Te.dispose(),z.dispose(),z.removeEventListener("sessionstart",rv),z.removeEventListener("sessionend",sv),Tr.stop()};function Y(_){_.preventDefault(),console.log("THREE.WebGLRenderer: Context Lost."),I=!0}function me(){console.log("THREE.WebGLRenderer: Context Restored."),I=!1;let _=Fe.autoReset,P=xe.enabled,k=xe.autoUpdate,U=xe.needsUpdate,O=xe.type;ue(),Fe.autoReset=_,xe.enabled=P,xe.autoUpdate=k,xe.needsUpdate=U,xe.type=O}function pe(_){console.error("THREE.WebGLRenderer: A WebGL context could not be created. Reason: ",_.statusMessage)}function We(_){let P=_.target;P.removeEventListener("dispose",We),Ct(P)}function Ct(_){Xt(_),he.remove(_)}function Xt(_){let P=he.get(_).programs;P!==void 0&&(P.forEach(function(k){Te.releaseProgram(k)}),_.isShaderMaterial&&Te.releaseShaderCache(_))}this.renderBufferDirect=function(_,P,k,U,O,ne){P===null&&(P=ut);let de=O.isMesh&&O.matrixWorld.determinant()<0,_e=$S(_,P,k,U,O);re.setMaterial(U,de);let Me=k.index,Be=1;if(U.wireframe===!0){if(Me=K.getWireframeAttribute(k),Me===void 0)return;Be=2}let Ve=k.drawRange,Re=k.attributes.position,at=Ve.start*Be,ft=(Ve.start+Ve.count)*Be;ne!==null&&(at=Math.max(at,ne.start*Be),ft=Math.min(ft,(ne.start+ne.count)*Be)),Me!==null?(at=Math.max(at,0),ft=Math.min(ft,Me.count)):Re!=null&&(at=Math.max(at,0),ft=Math.min(ft,Re.count));let It=ft-at;if(It<0||It===1/0)return;Et.setup(O,U,_e,k,Me);let Dt,lt=Ee;if(Me!==null&&(Dt=G.get(Me),lt=it,lt.setIndex(Dt)),O.isMesh)U.wireframe===!0?(re.setLineWidth(U.wireframeLinewidth*J()),lt.setMode(b.LINES)):lt.setMode(b.TRIANGLES);else if(O.isLine){let Oe=U.linewidth;Oe===void 0&&(Oe=1),re.setLineWidth(Oe*J()),O.isLineSegments?lt.setMode(b.LINES):O.isLineLoop?lt.setMode(b.LINE_LOOP):lt.setMode(b.LINE_STRIP)}else O.isPoints?lt.setMode(b.POINTS):O.isSprite&&lt.setMode(b.TRIANGLES);if(O.isBatchedMesh)if(O._multiDrawInstances!==null)lt.renderMultiDrawInstances(O._multiDrawStarts,O._multiDrawCounts,O._multiDrawCount,O._multiDrawInstances);else if(te.get("WEBGL_multi_draw"))lt.renderMultiDraw(O._multiDrawStarts,O._multiDrawCounts,O._multiDrawCount);else{let Oe=O._multiDrawStarts,jt=O._multiDrawCounts,ht=O._multiDrawCount,jn=Me?G.get(Me).bytesPerElement:1,Ms=he.get(U).currentProgram.getUniforms();for(let dn=0;dn<ht;dn++)Ms.setValue(b,"_gl_DrawID",dn),lt.render(Oe[dn]/jn,jt[dn])}else if(O.isInstancedMesh)lt.renderInstances(at,It,O.count);else if(k.isInstancedBufferGeometry){let Oe=k._maxInstanceCount!==void 0?k._maxInstanceCount:1/0,jt=Math.min(k.instanceCount,Oe);lt.renderInstances(at,It,jt)}else lt.render(at,It)};function mt(_,P,k){_.transparent===!0&&_.side===pi&&_.forceSinglePass===!1?(_.side=sn,_.needsUpdate=!0,Ic(_,P,k),_.side=Vi,_.needsUpdate=!0,Ic(_,P,k),_.side=pi):Ic(_,P,k)}this.compile=function(_,P,k=null){k===null&&(k=_),p=Qe.get(k),p.init(P),M.push(p),k.traverseVisible(function(O){O.isLight&&O.layers.test(P.layers)&&(p.pushLight(O),O.castShadow&&p.pushShadow(O))}),_!==k&&_.traverseVisible(function(O){O.isLight&&O.layers.test(P.layers)&&(p.pushLight(O),O.castShadow&&p.pushShadow(O))}),p.setupLights();let U=new Set;return _.traverse(function(O){if(!(O.isMesh||O.isPoints||O.isLine||O.isSprite))return;let ne=O.material;if(ne)if(Array.isArray(ne))for(let de=0;de<ne.length;de++){let _e=ne[de];mt(_e,k,O),U.add(_e)}else mt(ne,k,O),U.add(ne)}),M.pop(),p=null,U},this.compileAsync=function(_,P,k=null){let U=this.compile(_,P,k);return new Promise(O=>{function ne(){if(U.forEach(function(de){he.get(de).currentProgram.isReady()&&U.delete(de)}),U.size===0){O(_);return}setTimeout(ne,10)}te.get("KHR_parallel_shader_compile")!==null?ne():setTimeout(ne,10)})};let Gn=null;function yi(_){Gn&&Gn(_)}function rv(){Tr.stop()}function sv(){Tr.start()}let Tr=new zS;Tr.setAnimationLoop(yi),typeof self<"u"&&Tr.setContext(self),this.setAnimationLoop=function(_){Gn=_,z.setAnimationLoop(_),_===null?Tr.stop():Tr.start()},z.addEventListener("sessionstart",rv),z.addEventListener("sessionend",sv),this.render=function(_,P){if(P!==void 0&&P.isCamera!==!0){console.error("THREE.WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(I===!0)return;if(_.matrixWorldAutoUpdate===!0&&_.updateMatrixWorld(),P.parent===null&&P.matrixWorldAutoUpdate===!0&&P.updateMatrixWorld(),z.enabled===!0&&z.isPresenting===!0&&(z.cameraAutoUpdate===!0&&z.updateCamera(P),P=z.getCamera()),_.isScene===!0&&_.onBeforeRender(E,_,P,N),p=Qe.get(_,M.length),p.init(P),M.push(p),Ie.multiplyMatrices(P.projectionMatrix,P.matrixWorldInverse),W.setFromProjectionMatrix(Ie),we=this.localClippingEnabled,se=ie.init(this.clippingPlanes,we),m=ve.get(_,T.length),m.init(),T.push(m),z.enabled===!0&&z.isPresenting===!0){let ne=E.xr.getDepthSensingMesh();ne!==null&&Af(ne,P,-1/0,E.sortObjects)}Af(_,P,0,E.sortObjects),m.finish(),E.sortObjects===!0&&m.sort(ae,ge),ee=z.enabled===!1||z.isPresenting===!1||z.hasDepthSensing()===!1,ee&&Le.addToRenderList(m,_),this.info.render.frame++,se===!0&&ie.beginShadows();let k=p.state.shadowsArray;xe.render(k,_,P),se===!0&&ie.endShadows(),this.info.autoReset===!0&&this.info.reset();let U=m.opaque,O=m.transmissive;if(p.setupLights(),P.isArrayCamera){let ne=P.cameras;if(O.length>0)for(let de=0,_e=ne.length;de<_e;de++){let Me=ne[de];av(U,O,_,Me)}ee&&Le.render(_);for(let de=0,_e=ne.length;de<_e;de++){let Me=ne[de];ov(m,_,Me,Me.viewport)}}else O.length>0&&av(U,O,_,P),ee&&Le.render(_),ov(m,_,P);N!==null&&C===0&&(S.updateMultisampleRenderTarget(N),S.updateRenderTargetMipmap(N)),_.isScene===!0&&_.onAfterRender(E,_,P),Et.resetDefaultState(),w=-1,x=null,M.pop(),M.length>0?(p=M[M.length-1],se===!0&&ie.setGlobalState(E.clippingPlanes,p.state.camera)):p=null,T.pop(),T.length>0?m=T[T.length-1]:m=null};function Af(_,P,k,U){if(_.visible===!1)return;if(_.layers.test(P.layers)){if(_.isGroup)k=_.renderOrder;else if(_.isLOD)_.autoUpdate===!0&&_.update(P);else if(_.isLight)p.pushLight(_),_.castShadow&&p.pushShadow(_);else if(_.isSprite){if(!_.frustumCulled||W.intersectsSprite(_)){U&&De.setFromMatrixPosition(_.matrixWorld).applyMatrix4(Ie);let de=j.update(_),_e=_.material;_e.visible&&m.push(_,de,_e,k,De.z,null)}}else if((_.isMesh||_.isLine||_.isPoints)&&(!_.frustumCulled||W.intersectsObject(_))){let de=j.update(_),_e=_.material;if(U&&(_.boundingSphere!==void 0?(_.boundingSphere===null&&_.computeBoundingSphere(),De.copy(_.boundingSphere.center)):(de.boundingSphere===null&&de.computeBoundingSphere(),De.copy(de.boundingSphere.center)),De.applyMatrix4(_.matrixWorld).applyMatrix4(Ie)),Array.isArray(_e)){let Me=de.groups;for(let Be=0,Ve=Me.length;Be<Ve;Be++){let Re=Me[Be],at=_e[Re.materialIndex];at&&at.visible&&m.push(_,de,at,k,De.z,Re)}}else _e.visible&&m.push(_,de,_e,k,De.z,null)}}let ne=_.children;for(let de=0,_e=ne.length;de<_e;de++)Af(ne[de],P,k,U)}function ov(_,P,k,U){let O=_.opaque,ne=_.transmissive,de=_.transparent;p.setupLightsView(k),se===!0&&ie.setGlobalState(E.clippingPlanes,k),U&&re.viewport(A.copy(U)),O.length>0&&Ac(O,P,k),ne.length>0&&Ac(ne,P,k),de.length>0&&Ac(de,P,k),re.buffers.depth.setTest(!0),re.buffers.depth.setMask(!0),re.buffers.color.setMask(!0),re.setPolygonOffset(!1)}function av(_,P,k,U){if((k.isScene===!0?k.overrideMaterial:null)!==null)return;p.state.transmissionRenderTarget[U.id]===void 0&&(p.state.transmissionRenderTarget[U.id]=new ui(1,1,{generateMipmaps:!0,type:te.has("EXT_color_buffer_half_float")||te.has("EXT_color_buffer_float")?zo:mi,minFilter:wr,samples:4,stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:ct.workingColorSpace}));let ne=p.state.transmissionRenderTarget[U.id],de=U.viewport||A;ne.setSize(de.z*E.transmissionResolutionScale,de.w*E.transmissionResolutionScale);let _e=E.getRenderTarget();E.setRenderTarget(ne),E.getClearColor(q),X=E.getClearAlpha(),X<1&&E.setClearColor(16777215,.5),E.clear(),ee&&Le.render(k);let Me=E.toneMapping;E.toneMapping=ji;let Be=U.viewport;if(U.viewport!==void 0&&(U.viewport=void 0),p.setupLightsView(U),se===!0&&ie.setGlobalState(E.clippingPlanes,U),Ac(_,k,U),S.updateMultisampleRenderTarget(ne),S.updateRenderTargetMipmap(ne),te.has("WEBGL_multisampled_render_to_texture")===!1){let Ve=!1;for(let Re=0,at=P.length;Re<at;Re++){let ft=P[Re],It=ft.object,Dt=ft.geometry,lt=ft.material,Oe=ft.group;if(lt.side===pi&&It.layers.test(U.layers)){let jt=lt.side;lt.side=sn,lt.needsUpdate=!0,cv(It,k,U,Dt,lt,Oe),lt.side=jt,lt.needsUpdate=!0,Ve=!0}}Ve===!0&&(S.updateMultisampleRenderTarget(ne),S.updateRenderTargetMipmap(ne))}E.setRenderTarget(_e),E.setClearColor(q,X),Be!==void 0&&(U.viewport=Be),E.toneMapping=Me}function Ac(_,P,k){let U=P.isScene===!0?P.overrideMaterial:null;for(let O=0,ne=_.length;O<ne;O++){let de=_[O],_e=de.object,Me=de.geometry,Be=U===null?de.material:U,Ve=de.group;_e.layers.test(k.layers)&&cv(_e,P,k,Me,Be,Ve)}}function cv(_,P,k,U,O,ne){_.onBeforeRender(E,P,k,U,O,ne),_.modelViewMatrix.multiplyMatrices(k.matrixWorldInverse,_.matrixWorld),_.normalMatrix.getNormalMatrix(_.modelViewMatrix),O.onBeforeRender(E,P,k,U,_,ne),O.transparent===!0&&O.side===pi&&O.forceSinglePass===!1?(O.side=sn,O.needsUpdate=!0,E.renderBufferDirect(k,P,U,O,_,ne),O.side=Vi,O.needsUpdate=!0,E.renderBufferDirect(k,P,U,O,_,ne),O.side=pi):E.renderBufferDirect(k,P,U,O,_,ne),_.onAfterRender(E,P,k,U,O,ne)}function Ic(_,P,k){P.isScene!==!0&&(P=ut);let U=he.get(_),O=p.state.lights,ne=p.state.shadowsArray,de=O.state.version,_e=Te.getParameters(_,O.state,ne,P,k),Me=Te.getProgramCacheKey(_e),Be=U.programs;U.environment=_.isMeshStandardMaterial?P.environment:null,U.fog=P.fog,U.envMap=(_.isMeshStandardMaterial?F:y).get(_.envMap||U.environment),U.envMapRotation=U.environment!==null&&_.envMap===null?P.environmentRotation:_.envMapRotation,Be===void 0&&(_.addEventListener("dispose",We),Be=new Map,U.programs=Be);let Ve=Be.get(Me);if(Ve!==void 0){if(U.currentProgram===Ve&&U.lightsStateVersion===de)return uv(_,_e),Ve}else _e.uniforms=Te.getUniforms(_),_.onBeforeCompile(_e,E),Ve=Te.acquireProgram(_e,Me),Be.set(Me,Ve),U.uniforms=_e.uniforms;let Re=U.uniforms;return(!_.isShaderMaterial&&!_.isRawShaderMaterial||_.clipping===!0)&&(Re.clippingPlanes=ie.uniform),uv(_,_e),U.needsLights=XS(_),U.lightsStateVersion=de,U.needsLights&&(Re.ambientLightColor.value=O.state.ambient,Re.lightProbe.value=O.state.probe,Re.directionalLights.value=O.state.directional,Re.directionalLightShadows.value=O.state.directionalShadow,Re.spotLights.value=O.state.spot,Re.spotLightShadows.value=O.state.spotShadow,Re.rectAreaLights.value=O.state.rectArea,Re.ltc_1.value=O.state.rectAreaLTC1,Re.ltc_2.value=O.state.rectAreaLTC2,Re.pointLights.value=O.state.point,Re.pointLightShadows.value=O.state.pointShadow,Re.hemisphereLights.value=O.state.hemi,Re.directionalShadowMap.value=O.state.directionalShadowMap,Re.directionalShadowMatrix.value=O.state.directionalShadowMatrix,Re.spotShadowMap.value=O.state.spotShadowMap,Re.spotLightMatrix.value=O.state.spotLightMatrix,Re.spotLightMap.value=O.state.spotLightMap,Re.pointShadowMap.value=O.state.pointShadowMap,Re.pointShadowMatrix.value=O.state.pointShadowMatrix),U.currentProgram=Ve,U.uniformsList=null,Ve}function lv(_){if(_.uniformsList===null){let P=_.currentProgram.getUniforms();_.uniformsList=$o.seqWithValue(P.seq,_.uniforms)}return _.uniformsList}function uv(_,P){let k=he.get(_);k.outputColorSpace=P.outputColorSpace,k.batching=P.batching,k.batchingColor=P.batchingColor,k.instancing=P.instancing,k.instancingColor=P.instancingColor,k.instancingMorph=P.instancingMorph,k.skinning=P.skinning,k.morphTargets=P.morphTargets,k.morphNormals=P.morphNormals,k.morphColors=P.morphColors,k.morphTargetsCount=P.morphTargetsCount,k.numClippingPlanes=P.numClippingPlanes,k.numIntersection=P.numClipIntersection,k.vertexAlphas=P.vertexAlphas,k.vertexTangents=P.vertexTangents,k.toneMapping=P.toneMapping}function $S(_,P,k,U,O){P.isScene!==!0&&(P=ut),S.resetTextureUnits();let ne=P.fog,de=U.isMeshStandardMaterial?P.environment:null,_e=N===null?E.outputColorSpace:N.isXRRenderTarget===!0?N.texture.colorSpace:as,Me=(U.isMeshStandardMaterial?F:y).get(U.envMap||de),Be=U.vertexColors===!0&&!!k.attributes.color&&k.attributes.color.itemSize===4,Ve=!!k.attributes.tangent&&(!!U.normalMap||U.anisotropy>0),Re=!!k.morphAttributes.position,at=!!k.morphAttributes.normal,ft=!!k.morphAttributes.color,It=ji;U.toneMapped&&(N===null||N.isXRRenderTarget===!0)&&(It=E.toneMapping);let Dt=k.morphAttributes.position||k.morphAttributes.normal||k.morphAttributes.color,lt=Dt!==void 0?Dt.length:0,Oe=he.get(U),jt=p.state.lights;if(se===!0&&(we===!0||_!==x)){let tn=_===x&&U.id===w;ie.setState(U,_,tn)}let ht=!1;U.version===Oe.__version?(Oe.needsLights&&Oe.lightsStateVersion!==jt.state.version||Oe.outputColorSpace!==_e||O.isBatchedMesh&&Oe.batching===!1||!O.isBatchedMesh&&Oe.batching===!0||O.isBatchedMesh&&Oe.batchingColor===!0&&O.colorTexture===null||O.isBatchedMesh&&Oe.batchingColor===!1&&O.colorTexture!==null||O.isInstancedMesh&&Oe.instancing===!1||!O.isInstancedMesh&&Oe.instancing===!0||O.isSkinnedMesh&&Oe.skinning===!1||!O.isSkinnedMesh&&Oe.skinning===!0||O.isInstancedMesh&&Oe.instancingColor===!0&&O.instanceColor===null||O.isInstancedMesh&&Oe.instancingColor===!1&&O.instanceColor!==null||O.isInstancedMesh&&Oe.instancingMorph===!0&&O.morphTexture===null||O.isInstancedMesh&&Oe.instancingMorph===!1&&O.morphTexture!==null||Oe.envMap!==Me||U.fog===!0&&Oe.fog!==ne||Oe.numClippingPlanes!==void 0&&(Oe.numClippingPlanes!==ie.numPlanes||Oe.numIntersection!==ie.numIntersection)||Oe.vertexAlphas!==Be||Oe.vertexTangents!==Ve||Oe.morphTargets!==Re||Oe.morphNormals!==at||Oe.morphColors!==ft||Oe.toneMapping!==It||Oe.morphTargetsCount!==lt)&&(ht=!0):(ht=!0,Oe.__version=U.version);let jn=Oe.currentProgram;ht===!0&&(jn=Ic(U,P,O));let Ms=!1,dn=!1,Xo=!1,St=jn.getUniforms(),An=Oe.uniforms;if(re.useProgram(jn.program)&&(Ms=!0,dn=!0,Xo=!0),U.id!==w&&(w=U.id,dn=!0),Ms||x!==_){re.buffers.depth.getReversed()?(oe.copy(_.projectionMatrix),uS(oe),dS(oe),St.setValue(b,"projectionMatrix",oe)):St.setValue(b,"projectionMatrix",_.projectionMatrix),St.setValue(b,"viewMatrix",_.matrixWorldInverse);let on=St.map.cameraPosition;on!==void 0&&on.setValue(b,ot.setFromMatrixPosition(_.matrixWorld)),ye.logarithmicDepthBuffer&&St.setValue(b,"logDepthBufFC",2/(Math.log(_.far+1)/Math.LN2)),(U.isMeshPhongMaterial||U.isMeshToonMaterial||U.isMeshLambertMaterial||U.isMeshBasicMaterial||U.isMeshStandardMaterial||U.isShaderMaterial)&&St.setValue(b,"isOrthographic",_.isOrthographicCamera===!0),x!==_&&(x=_,dn=!0,Xo=!0)}if(O.isSkinnedMesh){St.setOptional(b,O,"bindMatrix"),St.setOptional(b,O,"bindMatrixInverse");let tn=O.skeleton;tn&&(tn.boneTexture===null&&tn.computeBoneTexture(),St.setValue(b,"boneTexture",tn.boneTexture,S))}O.isBatchedMesh&&(St.setOptional(b,O,"batchingTexture"),St.setValue(b,"batchingTexture",O._matricesTexture,S),St.setOptional(b,O,"batchingIdTexture"),St.setValue(b,"batchingIdTexture",O._indirectTexture,S),St.setOptional(b,O,"batchingColorTexture"),O._colorsTexture!==null&&St.setValue(b,"batchingColorTexture",O._colorsTexture,S));let In=k.morphAttributes;if((In.position!==void 0||In.normal!==void 0||In.color!==void 0)&&Ue.update(O,k,jn),(dn||Oe.receiveShadow!==O.receiveShadow)&&(Oe.receiveShadow=O.receiveShadow,St.setValue(b,"receiveShadow",O.receiveShadow)),U.isMeshGouraudMaterial&&U.envMap!==null&&(An.envMap.value=Me,An.flipEnvMap.value=Me.isCubeTexture&&Me.isRenderTargetTexture===!1?-1:1),U.isMeshStandardMaterial&&U.envMap===null&&P.environment!==null&&(An.envMapIntensity.value=P.environmentIntensity),dn&&(St.setValue(b,"toneMappingExposure",E.toneMappingExposure),Oe.needsLights&&qS(An,Xo),ne&&U.fog===!0&&le.refreshFogUniforms(An,ne),le.refreshMaterialUniforms(An,U,V,Z,p.state.transmissionRenderTarget[_.id]),$o.upload(b,lv(Oe),An,S)),U.isShaderMaterial&&U.uniformsNeedUpdate===!0&&($o.upload(b,lv(Oe),An,S),U.uniformsNeedUpdate=!1),U.isSpriteMaterial&&St.setValue(b,"center",O.center),St.setValue(b,"modelViewMatrix",O.modelViewMatrix),St.setValue(b,"normalMatrix",O.normalMatrix),St.setValue(b,"modelMatrix",O.matrixWorld),U.isShaderMaterial||U.isRawShaderMaterial){let tn=U.uniformsGroups;for(let on=0,If=tn.length;on<If;on++){let Cr=tn[on];R.update(Cr,jn),R.bind(Cr,jn)}}return jn}function qS(_,P){_.ambientLightColor.needsUpdate=P,_.lightProbe.needsUpdate=P,_.directionalLights.needsUpdate=P,_.directionalLightShadows.needsUpdate=P,_.pointLights.needsUpdate=P,_.pointLightShadows.needsUpdate=P,_.spotLights.needsUpdate=P,_.spotLightShadows.needsUpdate=P,_.rectAreaLights.needsUpdate=P,_.hemisphereLights.needsUpdate=P}function XS(_){return _.isMeshLambertMaterial||_.isMeshToonMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isShadowMaterial||_.isShaderMaterial&&_.lights===!0}this.getActiveCubeFace=function(){return D},this.getActiveMipmapLevel=function(){return C},this.getRenderTarget=function(){return N},this.setRenderTargetTextures=function(_,P,k){he.get(_.texture).__webglTexture=P,he.get(_.depthTexture).__webglTexture=k;let U=he.get(_);U.__hasExternalTextures=!0,U.__autoAllocateDepthBuffer=k===void 0,U.__autoAllocateDepthBuffer||te.has("WEBGL_multisampled_render_to_texture")===!0&&(console.warn("THREE.WebGLRenderer: Render-to-texture extension was disabled because an external texture was provided"),U.__useRenderToTexture=!1)},this.setRenderTargetFramebuffer=function(_,P){let k=he.get(_);k.__webglFramebuffer=P,k.__useDefaultFramebuffer=P===void 0};let YS=b.createFramebuffer();this.setRenderTarget=function(_,P=0,k=0){N=_,D=P,C=k;let U=!0,O=null,ne=!1,de=!1;if(_){let Me=he.get(_);if(Me.__useDefaultFramebuffer!==void 0)re.bindFramebuffer(b.FRAMEBUFFER,null),U=!1;else if(Me.__webglFramebuffer===void 0)S.setupRenderTarget(_);else if(Me.__hasExternalTextures)S.rebindTextures(_,he.get(_.texture).__webglTexture,he.get(_.depthTexture).__webglTexture);else if(_.depthBuffer){let Re=_.depthTexture;if(Me.__boundDepthTexture!==Re){if(Re!==null&&he.has(Re)&&(_.width!==Re.image.width||_.height!==Re.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");S.setupDepthRenderbuffer(_)}}let Be=_.texture;(Be.isData3DTexture||Be.isDataArrayTexture||Be.isCompressedArrayTexture)&&(de=!0);let Ve=he.get(_).__webglFramebuffer;_.isWebGLCubeRenderTarget?(Array.isArray(Ve[P])?O=Ve[P][k]:O=Ve[P],ne=!0):_.samples>0&&S.useMultisampledRTT(_)===!1?O=he.get(_).__webglMultisampledFramebuffer:Array.isArray(Ve)?O=Ve[k]:O=Ve,A.copy(_.viewport),$.copy(_.scissor),B=_.scissorTest}else A.copy(Se).multiplyScalar(V).floor(),$.copy(Ge).multiplyScalar(V).floor(),B=dt;if(k!==0&&(O=YS),re.bindFramebuffer(b.FRAMEBUFFER,O)&&U&&re.drawBuffers(_,O),re.viewport(A),re.scissor($),re.setScissorTest(B),ne){let Me=he.get(_.texture);b.framebufferTexture2D(b.FRAMEBUFFER,b.COLOR_ATTACHMENT0,b.TEXTURE_CUBE_MAP_POSITIVE_X+P,Me.__webglTexture,k)}else if(de){let Me=he.get(_.texture),Be=P;b.framebufferTextureLayer(b.FRAMEBUFFER,b.COLOR_ATTACHMENT0,Me.__webglTexture,k,Be)}else if(_!==null&&k!==0){let Me=he.get(_.texture);b.framebufferTexture2D(b.FRAMEBUFFER,b.COLOR_ATTACHMENT0,b.TEXTURE_2D,Me.__webglTexture,k)}w=-1},this.readRenderTargetPixels=function(_,P,k,U,O,ne,de){if(!(_&&_.isWebGLRenderTarget)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let _e=he.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&de!==void 0&&(_e=_e[de]),_e){re.bindFramebuffer(b.FRAMEBUFFER,_e);try{let Me=_.texture,Be=Me.format,Ve=Me.type;if(!ye.textureFormatReadable(Be)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!ye.textureTypeReadable(Ve)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}P>=0&&P<=_.width-U&&k>=0&&k<=_.height-O&&b.readPixels(P,k,U,O,Ye.convert(Be),Ye.convert(Ve),ne)}finally{let Me=N!==null?he.get(N).__webglFramebuffer:null;re.bindFramebuffer(b.FRAMEBUFFER,Me)}}},this.readRenderTargetPixelsAsync=function(_,P,k,U,O,ne,de){return _i(this,null,function*(){if(!(_&&_.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let _e=he.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&de!==void 0&&(_e=_e[de]),_e){let Me=_.texture,Be=Me.format,Ve=Me.type;if(!ye.textureFormatReadable(Be))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!ye.textureTypeReadable(Ve))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");if(P>=0&&P<=_.width-U&&k>=0&&k<=_.height-O){re.bindFramebuffer(b.FRAMEBUFFER,_e);let Re=b.createBuffer();b.bindBuffer(b.PIXEL_PACK_BUFFER,Re),b.bufferData(b.PIXEL_PACK_BUFFER,ne.byteLength,b.STREAM_READ),b.readPixels(P,k,U,O,Ye.convert(Be),Ye.convert(Ve),0);let at=N!==null?he.get(N).__webglFramebuffer:null;re.bindFramebuffer(b.FRAMEBUFFER,at);let ft=b.fenceSync(b.SYNC_GPU_COMMANDS_COMPLETE,0);return b.flush(),yield lS(b,ft,4),b.bindBuffer(b.PIXEL_PACK_BUFFER,Re),b.getBufferSubData(b.PIXEL_PACK_BUFFER,0,ne),b.deleteBuffer(Re),b.deleteSync(ft),ne}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")}})},this.copyFramebufferToTexture=function(_,P=null,k=0){_.isTexture!==!0&&(gs("WebGLRenderer: copyFramebufferToTexture function signature has changed."),P=arguments[0]||null,_=arguments[1]);let U=Math.pow(2,-k),O=Math.floor(_.image.width*U),ne=Math.floor(_.image.height*U),de=P!==null?P.x:0,_e=P!==null?P.y:0;S.setTexture2D(_,0),b.copyTexSubImage2D(b.TEXTURE_2D,k,0,0,de,_e,O,ne),re.unbindTexture()};let ZS=b.createFramebuffer(),KS=b.createFramebuffer();this.copyTextureToTexture=function(_,P,k=null,U=null,O=0,ne=null){_.isTexture!==!0&&(gs("WebGLRenderer: copyTextureToTexture function signature has changed."),U=arguments[0]||null,_=arguments[1],P=arguments[2],ne=arguments[3]||0,k=null),ne===null&&(O!==0?(gs("WebGLRenderer: copyTextureToTexture function signature has changed to support src and dst mipmap levels."),ne=O,O=0):ne=0);let de,_e,Me,Be,Ve,Re,at,ft,It,Dt=_.isCompressedTexture?_.mipmaps[ne]:_.image;if(k!==null)de=k.max.x-k.min.x,_e=k.max.y-k.min.y,Me=k.isBox3?k.max.z-k.min.z:1,Be=k.min.x,Ve=k.min.y,Re=k.isBox3?k.min.z:0;else{let In=Math.pow(2,-O);de=Math.floor(Dt.width*In),_e=Math.floor(Dt.height*In),_.isDataArrayTexture?Me=Dt.depth:_.isData3DTexture?Me=Math.floor(Dt.depth*In):Me=1,Be=0,Ve=0,Re=0}U!==null?(at=U.x,ft=U.y,It=U.z):(at=0,ft=0,It=0);let lt=Ye.convert(P.format),Oe=Ye.convert(P.type),jt;P.isData3DTexture?(S.setTexture3D(P,0),jt=b.TEXTURE_3D):P.isDataArrayTexture||P.isCompressedArrayTexture?(S.setTexture2DArray(P,0),jt=b.TEXTURE_2D_ARRAY):(S.setTexture2D(P,0),jt=b.TEXTURE_2D),b.pixelStorei(b.UNPACK_FLIP_Y_WEBGL,P.flipY),b.pixelStorei(b.UNPACK_PREMULTIPLY_ALPHA_WEBGL,P.premultiplyAlpha),b.pixelStorei(b.UNPACK_ALIGNMENT,P.unpackAlignment);let ht=b.getParameter(b.UNPACK_ROW_LENGTH),jn=b.getParameter(b.UNPACK_IMAGE_HEIGHT),Ms=b.getParameter(b.UNPACK_SKIP_PIXELS),dn=b.getParameter(b.UNPACK_SKIP_ROWS),Xo=b.getParameter(b.UNPACK_SKIP_IMAGES);b.pixelStorei(b.UNPACK_ROW_LENGTH,Dt.width),b.pixelStorei(b.UNPACK_IMAGE_HEIGHT,Dt.height),b.pixelStorei(b.UNPACK_SKIP_PIXELS,Be),b.pixelStorei(b.UNPACK_SKIP_ROWS,Ve),b.pixelStorei(b.UNPACK_SKIP_IMAGES,Re);let St=_.isDataArrayTexture||_.isData3DTexture,An=P.isDataArrayTexture||P.isData3DTexture;if(_.isDepthTexture){let In=he.get(_),tn=he.get(P),on=he.get(In.__renderTarget),If=he.get(tn.__renderTarget);re.bindFramebuffer(b.READ_FRAMEBUFFER,on.__webglFramebuffer),re.bindFramebuffer(b.DRAW_FRAMEBUFFER,If.__webglFramebuffer);for(let Cr=0;Cr<Me;Cr++)St&&(b.framebufferTextureLayer(b.READ_FRAMEBUFFER,b.COLOR_ATTACHMENT0,he.get(_).__webglTexture,O,Re+Cr),b.framebufferTextureLayer(b.DRAW_FRAMEBUFFER,b.COLOR_ATTACHMENT0,he.get(P).__webglTexture,ne,It+Cr)),b.blitFramebuffer(Be,Ve,de,_e,at,ft,de,_e,b.DEPTH_BUFFER_BIT,b.NEAREST);re.bindFramebuffer(b.READ_FRAMEBUFFER,null),re.bindFramebuffer(b.DRAW_FRAMEBUFFER,null)}else if(O!==0||_.isRenderTargetTexture||he.has(_)){let In=he.get(_),tn=he.get(P);re.bindFramebuffer(b.READ_FRAMEBUFFER,ZS),re.bindFramebuffer(b.DRAW_FRAMEBUFFER,KS);for(let on=0;on<Me;on++)St?b.framebufferTextureLayer(b.READ_FRAMEBUFFER,b.COLOR_ATTACHMENT0,In.__webglTexture,O,Re+on):b.framebufferTexture2D(b.READ_FRAMEBUFFER,b.COLOR_ATTACHMENT0,b.TEXTURE_2D,In.__webglTexture,O),An?b.framebufferTextureLayer(b.DRAW_FRAMEBUFFER,b.COLOR_ATTACHMENT0,tn.__webglTexture,ne,It+on):b.framebufferTexture2D(b.DRAW_FRAMEBUFFER,b.COLOR_ATTACHMENT0,b.TEXTURE_2D,tn.__webglTexture,ne),O!==0?b.blitFramebuffer(Be,Ve,de,_e,at,ft,de,_e,b.COLOR_BUFFER_BIT,b.NEAREST):An?b.copyTexSubImage3D(jt,ne,at,ft,It+on,Be,Ve,de,_e):b.copyTexSubImage2D(jt,ne,at,ft,Be,Ve,de,_e);re.bindFramebuffer(b.READ_FRAMEBUFFER,null),re.bindFramebuffer(b.DRAW_FRAMEBUFFER,null)}else An?_.isDataTexture||_.isData3DTexture?b.texSubImage3D(jt,ne,at,ft,It,de,_e,Me,lt,Oe,Dt.data):P.isCompressedArrayTexture?b.compressedTexSubImage3D(jt,ne,at,ft,It,de,_e,Me,lt,Dt.data):b.texSubImage3D(jt,ne,at,ft,It,de,_e,Me,lt,Oe,Dt):_.isDataTexture?b.texSubImage2D(b.TEXTURE_2D,ne,at,ft,de,_e,lt,Oe,Dt.data):_.isCompressedTexture?b.compressedTexSubImage2D(b.TEXTURE_2D,ne,at,ft,Dt.width,Dt.height,lt,Dt.data):b.texSubImage2D(b.TEXTURE_2D,ne,at,ft,de,_e,lt,Oe,Dt);b.pixelStorei(b.UNPACK_ROW_LENGTH,ht),b.pixelStorei(b.UNPACK_IMAGE_HEIGHT,jn),b.pixelStorei(b.UNPACK_SKIP_PIXELS,Ms),b.pixelStorei(b.UNPACK_SKIP_ROWS,dn),b.pixelStorei(b.UNPACK_SKIP_IMAGES,Xo),ne===0&&P.generateMipmaps&&b.generateMipmap(jt),re.unbindTexture()},this.copyTextureToTexture3D=function(_,P,k=null,U=null,O=0){return _.isTexture!==!0&&(gs("WebGLRenderer: copyTextureToTexture3D function signature has changed."),k=arguments[0]||null,U=arguments[1]||null,_=arguments[2],P=arguments[3],O=arguments[4]||0),gs('WebGLRenderer: copyTextureToTexture3D function has been deprecated. Use "copyTextureToTexture" instead.'),this.copyTextureToTexture(_,P,k,U,O)},this.initRenderTarget=function(_){he.get(_).__webglFramebuffer===void 0&&S.setupRenderTarget(_)},this.initTexture=function(_){_.isCubeTexture?S.setTextureCube(_,0):_.isData3DTexture?S.setTexture3D(_,0):_.isDataArrayTexture||_.isCompressedArrayTexture?S.setTexture2DArray(_,0):S.setTexture2D(_,0),re.unbindTexture()},this.resetState=function(){D=0,C=0,N=null,re.reset(),Et.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return li}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorspace=ct._getDrawingBufferColorSpace(e),t.unpackColorSpace=ct._getUnpackColorSpace()}};var bf=class extends Go{constructor(e){super(e)}load(e,t,i,r){let s=this,o=new mc(this.manager);o.setPath(this.path),o.setRequestHeader(this.requestHeader),o.setWithCredentials(this.withCredentials),o.load(e,function(a){let c=s.parse(JSON.parse(a));t&&t(c)},i,r)}parse(e){return new iv(e)}},iv=class{constructor(e){this.isFont=!0,this.type="Font",this.data=e}generateShapes(e,t=100){let i=[],r=f2(e,t,this.data);for(let s=0,o=r.length;s<o;s++)i.push(...r[s].toShapes());return i}};function f2(n,e,t){let i=Array.from(n),r=e/t.resolution,s=(t.boundingBox.yMax-t.boundingBox.yMin+t.underlineThickness)*r,o=[],a=0,c=0;for(let l=0;l<i.length;l++){let u=i[l];if(u===`
`)a=0,c-=s;else{let d=h2(u,r,a,c,t);a+=d.offsetX,o.push(d.path)}}return o}function h2(n,e,t,i,r){let s=r.glyphs[n]||r.glyphs["?"];if(!s){console.error('THREE.Font: character "'+n+'" does not exists in font family '+r.familyName+".");return}let o=new xc,a,c,l,u,d,f,h,g;if(s.o){let v=s._cachedOutline||(s._cachedOutline=s.o.split(" "));for(let m=0,p=v.length;m<p;)switch(v[m++]){case"m":a=v[m++]*e+t,c=v[m++]*e+i,o.moveTo(a,c);break;case"l":a=v[m++]*e+t,c=v[m++]*e+i,o.lineTo(a,c);break;case"q":l=v[m++]*e+t,u=v[m++]*e+i,d=v[m++]*e+t,f=v[m++]*e+i,o.quadraticCurveTo(d,f,l,u);break;case"b":l=v[m++]*e+t,u=v[m++]*e+i,d=v[m++]*e+t,f=v[m++]*e+i,h=v[m++]*e+t,g=v[m++]*e+i,o.bezierCurveTo(d,f,h,g,l,u);break}}return{offsetX:s.ha*e,path:o}}var Tf=class extends fc{constructor(e,t={}){let i=t.font;if(i===void 0)super();else{let r=i.generateShapes(e,t.size);t.depth===void 0&&(t.depth=50),t.bevelThickness===void 0&&(t.bevelThickness=10),t.bevelSize===void 0&&(t.bevelSize=8),t.bevelEnabled===void 0&&(t.bevelEnabled=!1),super(r,t)}this.type="TextGeometry"}};var Cf=class n{constructor(e,t){this.el=e;this.platformId=t}scene={};camera={};renderer={};globe={};isMouseDown=!1;previousMousePosition={x:0,y:0};angularVelocity={x:0,y:0};friction=.95;clickCount=0;shakeDuration=300;shakeIntensity=.1;textMesh=null;textVisible=!1;dogImageMesh=null;ngAfterViewInit(){Bp(this.platformId)&&(this.initThreeJS(),this.animate())}animate(){Bp(this.platformId)&&(requestAnimationFrame(()=>this.animate()),this.globe.rotation.y+=this.angularVelocity.y,this.globe.rotation.x+=this.angularVelocity.x,this.angularVelocity.x*=this.friction,this.angularVelocity.y*=this.friction,Math.abs(this.angularVelocity.x)<.001&&Math.abs(this.angularVelocity.y)<.001&&(this.angularVelocity.x=0,this.angularVelocity.y=0),this.textVisible&&(this.textMesh.visible=!0),this.renderer.render(this.scene,this.camera))}initThreeJS(){if(typeof window>"u"||typeof document>"u")return;this.scene=new ic,this.camera=new Qt(75,window.innerWidth/window.innerHeight,.1,1e3),this.camera.position.z=5,this.renderer=new Sf({alpha:!0}),this.renderer.setSize(window.innerWidth,window.innerHeight),this.el.nativeElement.appendChild(this.renderer.domElement),this.renderer.shadowMap.enabled=!0;let e=new us(2,32,32),t=new Bo({transparent:!0,opacity:1});new fs().load("assets/back-planet.jpg",a=>{t.map=a,t.needsUpdate=!0,this.renderer.render(this.scene,this.camera)},void 0,a=>{console.error("\u041E\u0448\u0438\u0431\u043A\u0430 \u043F\u0440\u0438 \u0437\u0430\u0433\u0440\u0443\u0437\u043A\u0435 \u0442\u0435\u043A\u0441\u0442\u0443\u0440\u044B:",a)}),this.globe=new Gt(e,t),this.globe.castShadow=!0,this.globe.receiveShadow=!0,this.scene.add(this.globe);let r=new _c(16777215,.5);this.scene.add(r);let s=new yc(16777215,1);s.position.set(5,5,5),s.castShadow=!0,this.scene.add(s),this.renderer.domElement.addEventListener("mousedown",this.onMouseDown.bind(this)),this.renderer.domElement.addEventListener("mouseup",this.onMouseUp.bind(this)),this.renderer.domElement.addEventListener("mousemove",this.onMouseMove.bind(this)),window.addEventListener("resize",this.onWindowResize.bind(this)),new bf().load("assets/helvetiker_regular.typeface.json",a=>{let c=new Tf("You are my universe",{font:a,size:1,depth:.3,curveSegments:12,bevelEnabled:!1}),l=new Er({color:16711680,transparent:!0,opacity:0});this.textMesh=new Gt(c,l),this.textMesh.position.set(-7,-5,-20),this.scene.add(this.textMesh),new fs().load("assets/doggy.png",d=>{let f=new ls(10,10),h=new Er({map:d,transparent:!0});this.dogImageMesh=new Gt(f,h),this.dogImageMesh.position.set(0,0,-20),this.scene.add(this.dogImageMesh)},void 0,d=>{console.error("\u041E\u0448\u0438\u0431\u043A\u0430 \u043F\u0440\u0438 \u0437\u0430\u0433\u0440\u0443\u0437\u043A\u0435 \u0438\u0437\u043E\u0431\u0440\u0430\u0436\u0435\u043D\u0438\u044F \u0441\u043E\u0431\u0430\u043A\u0438:",d)})},void 0,a=>{console.error("\u041E\u0448\u0438\u0431\u043A\u0430 \u043F\u0440\u0438 \u0437\u0430\u0433\u0440\u0443\u0437\u043A\u0435 \u0448\u0440\u0438\u0444\u0442\u0430:",a)})}onMouseDown(e){this.isMouseDown=!0,this.previousMousePosition={x:e.clientX,y:e.clientY},this.clickCount++,this.clickCount<=4&&this.shakePlanet(),this.clickCount===4&&(this.splitPlanet(),this.textVisible=!0,this.fadeInText())}splitPlanet(){this.scene.remove(this.globe);let e=new Bo({transparent:!0,opacity:1});new fs().load("assets/back-planet.jpg",u=>{e.map=u,e.needsUpdate=!0,this.renderer.render(this.scene,this.camera)},void 0,u=>{console.error("\u041E\u0448\u0438\u0431\u043A\u0430 \u043F\u0440\u0438 \u0437\u0430\u0433\u0440\u0443\u0437\u043A\u0435 \u0442\u0435\u043A\u0441\u0442\u0443\u0440\u044B:",u)});let i=2,r=600,s=600,o=new us(i,r,s,0,Math.PI*2,0,Math.PI/2),a=new Gt(o,e);a.position.copy(this.globe.position),a.rotation.x=Math.PI/2,a.rotation.z=Math.PI/2;let c=new us(i,r,s,0,Math.PI*2,Math.PI/2,Math.PI),l=new Gt(c,e);l.position.copy(this.globe.position),l.rotation.x=Math.PI/2,l.rotation.z=Math.PI/2,this.scene.add(a),this.scene.add(l),this.animateGlobePart(a,new L(-5,0,-3)),this.animateGlobePart(l,new L(5,0,3))}animateGlobePart(e,t){let o=0,a=e.position.clone(),c=setInterval(()=>{o<=1e3?(e.position.x=a.x+t.x*(o/1e3)*4,e.position.y=a.y,e.position.z=a.z,o++):(clearInterval(c),this.scene.remove(e))},5)}fadeInText(){if(this.textMesh&&this.textMesh.material){let e=this.textMesh.material;e.opacity=0;let t=4e3,i=100,r=t/i,s=0,o=setInterval(()=>{s<=i?(e.opacity=s/i,s++):clearInterval(o)},r)}}onMouseUp(e){this.isMouseDown=!1,this.angularVelocity.y=(this.previousMousePosition.x-e.clientX)*.01,this.angularVelocity.x=(this.previousMousePosition.y-e.clientY)*.01}onMouseMove(e){if(!this.isMouseDown)return;let t={x:e.clientX-this.previousMousePosition.x,y:e.clientY-this.previousMousePosition.y};this.globe.rotation.y+=t.x*.01,this.globe.rotation.x+=t.y*.01,this.previousMousePosition={x:e.clientX,y:e.clientY}}shakePlanet(){let e=this.globe.position.clone(),t=setInterval(()=>{this.globe.position.x=e.x+(Math.random()-.5)*this.shakeIntensity,this.globe.position.y=e.y+(Math.random()-.5)*this.shakeIntensity},50);setTimeout(()=>{clearInterval(t),this.globe.position.copy(e)},this.shakeDuration)}onWindowResize(){this.camera.aspect=window.innerWidth/window.innerHeight,this.camera.updateProjectionMatrix(),this.renderer.setSize(window.innerWidth,window.innerHeight)}static \u0275fac=function(t){return new(t||n)(ro(to),ro(Ii))};static \u0275cmp=so({type:n,selectors:[["app-planet"]],decls:3,vars:0,consts:[[1,"planet-container"],[1,"video-background"],["src","assets/galaxy.gif","alt","Galaxy Background","autoplay","","loop","","muted","",1,"video-background-gif"]],template:function(t,i){t&1&&(ar(0,"div",0),tu(1,"div",1),ar(2,"img",2),nu())},styles:["[_nghost-%COMP%]{display:block;height:100vh;width:100vw;user-select:none;-webkit-user-select:none;-moz-user-select:none;-ms-user-select:none}.video-background[_ngcontent-%COMP%]{position:absolute;top:0;left:0;width:100%;height:100%;overflow:hidden;z-index:-1}.video-background-gif[_ngcontent-%COMP%]{min-width:100%;min-height:100%;object-fit:cover}"]})};var Df=class n{title="planet";static \u0275fac=function(t){return new(t||n)};static \u0275cmp=so({type:n,selectors:[["app-root"]],decls:1,vars:0,template:function(t,i){t&1&&ar(0,"app-planet")},dependencies:[Cf],encapsulation:2})};lE(Df,KE).catch(n=>console.error(n));
