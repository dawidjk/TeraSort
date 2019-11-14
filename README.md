<h1 id="tera-sort">Tera Sort</h1>
<p>The goal of <em>Tera Sort</em> was to learn about distributed systems. In order to achieve this, I set out in a mission to make my own little cluster. As soon as the Raspberry Pi 4 had launched, I purchased 4 - eventually acquiring 4 more (my cluster is now at 8 woot!). Together with 5 terabytes worth of disk storage, we can now benchmark the system… But how do we do such a thing you may mask?<br>
…<br>
Easy!<br>
…<br>
By sorting a terabyte of random 64 bit binary encoded integers. And so, I introduce: <em>Tera Sort</em></p>
<h1 id="the-setup">The Setup</h1>
<h2 id="specs">Specs</h2>
<p><strong>8x</strong> Raspberry Pi 4 (4gb) - 32gb RAM @ 1.5GHz/node<br>
<strong>2x</strong> Gigabit  network switches<br>
<strong>3x</strong> 1 TB HDD<br>
<strong>1x</strong> 2 TB HDD</p>
<h2 id="in-progress">In Progress:</h2>
<ul>
<li>Merging files after in memory sort</li>
</ul>
<h2 id="todo">TODO:</h2>
<ul>
<li>Network Communication</li>
<li>Kubernetes Orchestration</li>
<li>GitHub scoreboard</li>
</ul>
<h2 id="done">Done:</h2>
<ul>
<li>Benchmark timing</li>
<li>In memory sort optimization (turns out Go does a pretty good job)</li>
<li>Load integers from binary file</li>
<li>Create 1 TB of random integers and store as binary</li>
</ul>

